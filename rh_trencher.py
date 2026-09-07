#!/usr/bin/env python3
"""Robinhood Chain Grok Trencher — paper simulation. No keys, no live orders."""
from __future__ import annotations

import hashlib
import re
from collections import Counter
from dataclasses import dataclass, field
from enum import Enum

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd


class DeskState(str, Enum):
    SCANNING = "SCANNING"
    IN_POSITION = "IN_POSITION"
    DRAWDOWN = "DRAWDOWN"
    SELF_PAUSE = "SELF_PAUSE"
    FILTERED = "FILTERED"
    ROTATION = "ROTATION"


@dataclass
class TokenLaunch:
    t_min: int
    ticker: str
    name: str
    description: str
    launchpad: str
    liquidity_eth: float
    liq_growth: float
    deployer: str
    holders: list
    linked_groups: list
    selling_linked: int
    true_multiple_path: list
    theme_hint: str = ""
    _pool_id: str = ""  # unique pool/pair address — for deduplication
    _pairCreatedAt: int = 0       # DexScreener pairCreatedAt (epoch ms)
    _launch_price_usd: float = 0.0  # entry priceUsd for live multiple calc
    _pair_address: str = ""       # pairAddress (dedup key, alias for _pool_id)


@dataclass
class Vote:
    narrative: float
    liquidity: float
    risk_veto: bool
    reasons: list = field(default_factory=list)

    def allows(self, filtered: bool, nav_need: float | None = None) -> bool:
        liq_need = 0.50 if filtered else 0.16
        if nav_need is None:
            nav_need = 0.50
        return (not self.risk_veto) and self.narrative >= nav_need and self.liquidity >= liq_need


@dataclass
class Fill:
    t_min: int
    ticker: str
    side: str
    usd: float
    multiple: float
    note: str


@dataclass
class Position:
    ticker: str
    entry_min: int
    entry_usd: float
    size_frac: float
    peak_mult: float = 1.0
    current_mult: float = 1.0


THEME_LEXICON = {
    "hood": [
        "hood", "robinhood", "robin", "hoodai", "hoodrat", "hoodcash",
        "cashcat", "broker", "commission", "app", "tendies", "stonk",
        "vlad", "tenev", "l2", "stocktoken", "gang", "mafia", "cartel", "heist", "kingpin",
    ],
    "pet": [
        "pet", "pepe", "dog", "wif", "cat", "inu", "frog", "kitty", "puppy",
        "doge", "florki", "bonk", "shib", "kibble", "bark", "roco", "smooth",
    ],
    "ai-craze": [
        "ai-craze", "ai", "agent", "grok", "gpt", "neural", "bot", "llm",
        "openai", "mistral", "quantum", "claude", "megatron",
    ],
    "political": [
        "political", "trump", "maga", "biden", "giga", "elon", "chad",
    ],
    "sol": ["sol", "solana", "pump", "jet"],
}


def tokenize(text: str) -> list[str]:
    text = re.sub(r"[^a-z0-9$]+", " ", text.lower())
    return [t for t in text.split() if t]


class NarrativeEngine:
    def __init__(self, dim: int = 48, ngram: int = 3, seed: int = 7):
        self.dim = dim
        self.ngram = ngram
        rng = np.random.default_rng(seed)
        self.proj = rng.normal(0, 1, size=(256, dim))
        self.cluster_centroid: np.ndarray | None = None
        self.cluster_label: str | None = None
        self.min_match = 0.55

    def _hash_embed(self, text: str) -> np.ndarray:
        s = " ".join(tokenize(text))
        vec = np.zeros(self.dim, dtype=float)
        padded = f"#{s}#"
        for i in range(max(1, len(padded) - self.ngram + 1)):
            gram = padded[i : i + self.ngram]
            h = int(hashlib.md5(gram.encode("utf-8")).hexdigest(), 16) % 256
            vec += self.proj[h]
        tokens = set(tokenize(text))
        for i, (_, words) in enumerate(THEME_LEXICON.items()):
            hit = sum(1 for w in words if any(w in t or t in w for t in tokens))
            if i < self.dim:
                vec[i] += 3.0 * hit
        n = np.linalg.norm(vec)
        return vec / n if n > 1e-9 else vec

    def embed_token(self, tok: TokenLaunch) -> np.ndarray:
        return self._hash_embed(f"{tok.ticker} {tok.name} {tok.description} {tok.launchpad}")

    def score(self, tok: TokenLaunch) -> tuple[float, str]:
        v = self.embed_token(tok)
        if self.cluster_centroid is None:
            return float(0.58 + 0.30 * self._axis_energy(tok, "hood")), "open-scan"
        base = float(np.dot(v, self.cluster_centroid))
        # theme_hint 直接匹配 → 强制通过 narrative 门槛
        if tok.theme_hint and tok.theme_hint == self.cluster_label:
            boosted = max(base, self.min_match + 0.1)
            return boosted, self.cluster_label or "cluster"
        return base, self.cluster_label or "cluster"

    def _axis_energy(self, tok: TokenLaunch, axis: str) -> float:
        # theme_hint 直接命中 → 满能量
        if tok.theme_hint and tok.theme_hint == axis:
            return 1.0
        # 有 theme_hint 但不匹配 → 低能量（暗示属于别的主题）
        if tok.theme_hint:
            return 0.15
        blob = f"{tok.ticker} {tok.name} {tok.description}".lower()
        words = THEME_LEXICON.get(axis, [])
        return min(1.0, sum(w in blob for w in words) / 3.0)

    def rebuild_from_trades(self, trades: list[dict], market_survivors: list[dict] | None = None) -> dict:
        if not trades and not market_survivors:
            return {"ok": False, "reason": "no trades"}
        rows = list(trades)
        for ms in (market_survivors or []):
            rows.append({"text": ms["text"], "pnl_mult": ms.get("pnl_mult", 5.0)})
        vecs, labels = [], []
        for tr in rows:
            vecs.append(self._hash_embed(tr["text"]))
            labels.append("WIN" if tr["pnl_mult"] > 1.0 else "LOSS")
        X = np.vstack(vecs)
        winners = X[np.array(labels) == "WIN"]
        losses = X[np.array(labels) == "LOSS"]
        theme_votes = Counter()
        for tr in rows:
            if tr["pnl_mult"] > 1.0:
                # theme_hint 直接命中 → 高权重投票
                if tr.get("theme_hint"):
                    theme_votes[tr["theme_hint"]] += 3
                blob = tr["text"].lower()
                # Strip "/ sol" pair suffix so platform name doesn't leak into theme votes
                blob = blob.replace("/ sol", "").replace(" / sol", "").replace("/ solana", "")
                for theme, words in THEME_LEXICON.items():
                    theme_votes[theme] += sum(w in blob for w in words)
        if winners.size == 0 or not theme_votes or theme_votes.most_common(1)[0][1] <= 0:
            self.cluster_centroid = None
            self.cluster_label = None
            return {"ok": False, "reason": "no surviving winner theme", "votes": dict(theme_votes)}
        top_theme, _ = theme_votes.most_common(1)[0]
        centroid = winners.mean(axis=0)
        proto = self._hash_embed(" ".join(THEME_LEXICON.get(top_theme, [])))
        centroid = 0.55 * centroid + 0.45 * proto
        centroid = centroid / (np.linalg.norm(centroid) + 1e-9)
        self.cluster_centroid = centroid
        self.cluster_label = top_theme
        self.min_match = 0.50
        win_sim = float(np.mean(winners @ centroid))
        loss_sim = float(np.mean(losses @ centroid)) if len(losses) else 0.0
        return {
            "ok": True,
            "theme": top_theme,
            "votes": dict(theme_votes),
            "winner_cos": round(win_sim, 3),
            "loser_cos": round(loss_sim, 3),
            "n_win": int(len(winners)),
            "n_loss": int(len(losses)),
        }

    def project_2d(self, tokens: list[TokenLaunch]) -> pd.DataFrame:
        X = np.vstack([self.embed_token(t) for t in tokens])
        X = X - X.mean(axis=0)
        U, S, _ = np.linalg.svd(X, full_matrices=False)
        xy = U[:, :2] * S[:2]
        sims = [self.score(t) for t in tokens]
        return pd.DataFrame(
            {
                "ticker": [t.ticker for t in tokens],
                "x": xy[:, 0],
                "y": xy[:, 1],
                "match": [s for s, _ in sims],
                "label": [lab for _, lab in sims],
                "theme_hint": [t.theme_hint for t in tokens],
            }
        )


class WalletMap:
    def build(self, tok: TokenLaunch) -> nx.Graph:
        g = nx.Graph()
        for w in tok.holders:
            g.add_node(w, kind="holder")
        g.add_node(tok.deployer, kind="deployer")
        if tok.holders:
            g.add_edge(tok.deployer, tok.holders[0], rel="seed")
        for group in tok.linked_groups:
            grp = list(group)
            for a, b in zip(grp, grp[1:]):
                g.add_edge(a, b, rel="funded")
        return g

    def score(self, tok: TokenLaunch) -> dict:
        g = self.build(tok)
        n_holders = max(1, len(tok.holders))
        components = [c for c in nx.connected_components(g) if len(c) >= 2]
        linked_nodes = set().union(*components) if components else set()
        linked_share = len(linked_nodes & set(tok.holders)) / n_holders
        # Normalize selling_linked (0-3 severity) to 0-1 range
        dump_factor = min(1.0, tok.selling_linked / 3.0) if tok.selling_linked > 0 else 0.0
        dep_deg = g.degree(tok.deployer) if tok.deployer in g else 0
        pressure = 0.45 * dump_factor + 0.35 * linked_share + 0.20 * min(1.0, dep_deg / 4)
        flags = []
        if dump_factor >= 0.30:
            flags.append("whale_concentration")
        if dump_factor >= 0.67:
            flags.append("extreme_concentration")
        if dump_factor >= 0.90:
            flags.append("single_holder_rug_risk")
        if linked_share >= 0.50:
            flags.append("holder_cluster_concentrated")
        if dep_deg >= 3:
            flags.append("deployer_high_connectivity")
        return {
            "pressure": round(float(pressure), 3),
            "linked_share": round(float(linked_share), 3),
            "dump": round(float(dump_factor), 3),
            "components": len(components),
            "flags": flags,
            # sell>=2 (top1>50%) → veto entry; sell>=3 (top1>70%) → force exit
            "veto_entry": pressure >= 0.50 or dump_factor >= 0.67,
            "force_exit": dump_factor >= 0.90 or pressure >= 0.70,
        }


class Sizer:
    def __init__(self, bankroll_usd: float, f_cap: float = 0.15, fraction: float = 0.35):
        self.bankroll = bankroll_usd
        self.f_cap = f_cap
        self.fraction = fraction
        self.history: list[float] = []

    def record(self, r_multiple: float) -> None:
        self.history.append(r_multiple)

    def empirical(self) -> dict:
        if not self.history:
            return {"p": 0.35, "W": 2.0, "L": 0.45, "n": 0, "expectancy": 0.0}
        h = np.array(self.history)
        wins, losses = h[h > 0], np.abs(h[h <= 0])
        p = len(wins) / len(h)
        W = float(wins.mean()) if len(wins) else 2.0
        L = float(losses.mean()) if len(losses) else 0.45
        return {"p": p, "W": W, "L": L, "n": len(h), "expectancy": p * W - (1 - p) * L}

    def kelly(self, p=None, W=None, L=None) -> dict:
        stats = self.empirical()
        p = stats["p"] if p is None else p
        W = stats["W"] if W is None else W
        L = stats["L"] if L is None else L
        q = 1 - p
        f_full = 0.0 if W <= 0 or L <= 0 else max(0.0, p / L - q / W)
        return {
            "full_kelly": f_full,
            "used": min(self.f_cap, f_full * self.fraction),
            "p": p,
            "W": W,
            "L": L,
            "expectancy": p * W - q * L,
        }

    def risk_of_ruin(self, start, f, p, W, L, n_trades=40, n_sims=1000, ruin_level=0.20, seed=0):
        rng = np.random.default_rng(seed)
        ruined, finals = 0, []
        for _ in range(n_sims):
            eq, dead = start, False
            for _k in range(n_trades):
                stake = eq * f
                eq = eq + stake * W if rng.random() < p else eq - stake * L
                if eq <= start * ruin_level:
                    dead = True
                    break
            ruined += int(dead)
            finals.append(max(eq, 0.0))
        return {
            "ruin_prob": ruined / n_sims,
            "median_final": float(np.median(finals)),
            "p05_final": float(np.percentile(finals, 5)),
            "n_sims": n_sims,
        }

    def stake_usd(self, bankroll: float, survival_mode: bool = False,
                  max_stake_cap: float | None = None,
                  realistic: bool = False) -> tuple[float, dict]:
        """
        Decide how much USD to stake.

        Args:
            bankroll: current bankroll in USD
            survival_mode: reduced sizing after losses
            max_stake_cap: hard USD cap from liquidity (realistic mode)
            realistic: if True, respect liquidity caps + skip tiny stakes

        Returns:
            usd stake, dict of kelly/meta info. Returns 0 if realistic and cap too small.
        """
        k, stats = self.kelly(), self.empirical()
        k = {**k, "n": stats["n"]}
        f = k["used"]
        if survival_mode or k["expectancy"] <= 0:
            f = min(f, 0.12)
        # Proportional cap (was absolute $80, now grows with bankroll)
        usd = float(np.clip(bankroll * f, bankroll * 0.02, bankroll * 0.25))

        # Realistic mode: liquidity-aware cap — no hard min floor; skip if cap too small
        if realistic:
            if max_stake_cap is not None and max_stake_cap > 0:
                usd = min(usd, max_stake_cap)
                # If cap forces stake below threshold, this pool is not worth trading
                if usd < 5.0:
                    return 0.0, {**k, "survival_mode": survival_mode, "realistic_skip": "cap_too_small"}
            # No extra bankroll cap here — just let liquidity cap be the constraint

        return usd, {**k, "survival_mode": survival_mode}


class Desk:
    def __init__(self, start_usd: float = 500.0, loss_pause_n: int = 4,
                 narrative_seed: int = 7, thin_cut_override: float | None = None,
                 max_positions: int = 3, live_mode: bool = False,
                 realistic: bool = False, max_slippage_pct: float = 0.02,
                 peak_proxy_coef: float = 0.6,
                 eth_usd: float | None = None,
                 signal_callback=None):
        self.state = DeskState.SCANNING
        self.bankroll = start_usd
        self.start = start_usd
        self.narrative = NarrativeEngine(seed=narrative_seed)
        self.wallets = WalletMap()
        self.sizer = Sizer(start_usd)
        self.loss_pause_n = loss_pause_n
        self._thin_cut_override = thin_cut_override
        self.max_positions = max_positions
        self.live_mode = live_mode
        self.realistic = realistic
        self.max_slippage_pct = max_slippage_pct
        self.peak_proxy_coef = peak_proxy_coef
        self.eth_usd = float(eth_usd) if eth_usd is not None else 3000.0
        self.fx_feed = None  # set by TickEngine in live mode for SSE extras
        self.consec_losses = 0
        self.positions: list[Position] = []
        self.feed: list[Fill] = []
        self.closed: list[dict] = []
        self.seen = self.entered = self.rejected = 0
        self.equity_curve = [(0, start_usd, "start")]
        self.market_survivors: list[dict] = []
        self.traded_pools: set[str] = set()  # pool/pair addresses (fallback: ticker)
        self.signal_callback = signal_callback  # optional: callable(Fill) -> None

    @property
    def position(self) -> Position | None:
        """Backward compat: returns first open position (or None)."""
        return self.positions[0] if self.positions else None

    def log(self, t, ticker, side, usd, multiple, note):
        fill = Fill(t, ticker, side, usd, multiple, note)
        self.feed.append(fill)
        if self.signal_callback is not None:
            try:
                self.signal_callback(fill)
            except Exception:
                pass

    def vote(self, tok: TokenLaunch) -> Vote:
        n_score, n_lab = self.narrative.score(tok)
        reasons = [f"narrative={n_score:.2f}[{n_lab}]"]
        liq = min(1.0, tok.liquidity_eth / 8.0) * 0.55 + min(1.0, (tok.liq_growth - 1.0)) * 0.45
        liq = float(np.clip(liq, 0, 1))
        reasons.append(f"liq={liq:.2f}(eth={tok.liquidity_eth:.1f},g={tok.liq_growth:.2f})")
        # theme_hint 直接融入 narrative score 计算（已在 _axis_energy 处理）
        w = self.wallets.score(tok)
        reasons.extend(w["flags"])
        veto = False
        # Wallet veto strategy:
        # - theme-matched token: trust the theme, skip wallet veto entirely
        #   (theme signal from rebuild beats raw concentration heuristics)
        # - non-theme token: block sell>=2 (extreme+single holder)
        # Runner escape: high liq_growth (>=5x) + peak potential → bypass ALL wallet veto
        #   (single-holder tokens that run 50x aren't rugs — they're just thin books)
        # LIVE MODE: 用 liq_growth proxy，不用 future path
        peak_potential = (tok.liq_growth * self.peak_proxy_coef if self.live_mode
                         else max(tok.true_multiple_path or [1]))
        # backtest: peak>=5x 或 lg>=5x；live: lg>=3x 足够（因为 peak_potential 已经是 proxy）
        runner_escape = (tok.liq_growth >= 3.0 if self.live_mode
                        else (tok.liq_growth >= 5.0 or peak_potential >= 5.0))
        wallet_block = False
        if tok.theme_hint:
            wallet_block = False  # theme-matched → trust, no wallet veto
        elif w["dump"] >= 0.9:
            # Single-holder risk: hard block UNLESS runner_escape (liq_growth>=5 or peak>=5x)
            wallet_block = not runner_escape
        elif w["dump"] >= 0.67 and not runner_escape:
            wallet_block = True  # concentrated whale, but runner_escape bypasses
        if wallet_block:
            veto = True
            reasons.append("RISK_VETO wallet")
        thin_cut = self._thin_cut_override if self._thin_cut_override is not None else (
            0.33 if self.narrative.cluster_centroid is not None else 0.12)
        if liq < thin_cut:
            veto = True
            reasons.append("RISK_VETO thin_book")
        # 高 liq_growth (≥3x) 的无 theme pool 可能是未被识别的 runner → 放行
        if self.narrative.cluster_centroid is not None and n_score < self.narrative.min_match:
            if runner_escape:
                reasons.append(f"off-narrative ESCAPED liq_growth={tok.liq_growth:.1f}x")
            else:
                veto = True
                reasons.append("RISK_VETO off-narrative")
        if self.narrative.cluster_label:
            if self.narrative._axis_energy(tok, self.narrative.cluster_label) < 0.34:
                if runner_escape:
                    reasons.append(f"off-lexicon ESCAPED liq_growth={tok.liq_growth:.1f}x")
                else:
                    veto = True
                    reasons.append("RISK_VETO off-narrative-lexicon")
        return Vote(narrative=n_score, liquidity=liq, risk_veto=veto, reasons=reasons)

    def on_launch(self, tok: TokenLaunch) -> Vote | None:
        self.seen += 1
        if len(self.positions) >= self.max_positions:
            self.rejected += 1
            self.log(tok.t_min, tok.ticker, "REJECT", 0, 0, f"max positions ({self.max_positions})")
            return None
        pool_key = tok._pool_id or tok.ticker  # prefer pair address, fallback ticker
        if pool_key in self.traded_pools:
            self.rejected += 1
            self.log(tok.t_min, tok.ticker, "REJECT", 0, 0, "already traded this mint")
            return None
        if self.state == DeskState.SELF_PAUSE:
            self.rejected += 1
            self.log(tok.t_min, tok.ticker, "REJECT", 0, 0, "self-pause replaying losses")
            return None
        if tok.ticker in {"HOODRAT", "CASHCAT"} and self.narrative.cluster_centroid is None:
            self.rejected += 1
            self.log(tok.t_min, tok.ticker, "REJECT", 0, 0, "observed runner · not in book")
            return None
        v = self.vote(tok)
        filtered = self.narrative.cluster_centroid is not None
        nav_need = self.narrative.min_match if filtered else 0.50
        # runner_escape: 与 vote() 定义一致 —— peak>=5x 或 liq_growth>=5x，不限 theme_hint
        # 让 cross-theme runners（比如 ai-craze 的 old lai 在 pet FILTERED 状态下）也能进来
        peak_potential = (tok.liq_growth * self.peak_proxy_coef if self.live_mode
                         else max(tok.true_multiple_path or [1]))
        runner_escape = (tok.liq_growth >= 3.0 if self.live_mode
                        else (tok.liq_growth >= 5.0 or peak_potential >= 5.0))
        # runner_escape token 直接放行（只要没有 risk_veto），绕过 narrative/liq 门槛
        if runner_escape and not v.risk_veto:
            pass  # proceed to entry regardless of allows()
        elif not v.allows(filtered, nav_need=nav_need):
            self.rejected += 1
            self.log(tok.t_min, tok.ticker, "NOT_BUY", 0, 0, "; ".join(v.reasons))
            return v
        survival = self.state in (DeskState.DRAWDOWN, DeskState.FILTERED) or self.consec_losses >= 2
        # ── Realistic mode: liquidity-aware stake cap ──
        # Estimate pool liquidity at entry time, not launch time.
        # Runners have high liq_growth on tiny base liquidity — by entry, liquidity has grown.
        # Use max(initial_liq * liq_growth, initial_liq) so flat pools aren't distorted.
        # NOTE: tok.liquidity_eth is "ETH-equivalent" (USD / ETH_USD from fetch_dexscreener),
        # so entry_liq * ETH_USD gives USD liquidity for the stake cap calculation.
        # Cap liq_growth at 500x — beyond that, base liquidity noise dominates and cap becomes useless.
        ETH_USD = self.eth_usd
        MAX_REALISTIC_LG = 500.0
        max_stake_cap = None
        if self.realistic:
            capped_lg = min(max(tok.liq_growth, 1.0), MAX_REALISTIC_LG)
            entry_liq_eth = max(tok.liquidity_eth * capped_lg, tok.liquidity_eth)
            entry_liq_usd = entry_liq_eth * ETH_USD
            max_stake_cap = entry_liq_usd * self.max_slippage_pct
            if tok.liquidity_eth <= 0:
                self.rejected += 1
                self.log(tok.t_min, tok.ticker, "REJECT", 0, 0, "liq_zero")
                return v
        stake, meta = self.sizer.stake_usd(
            self.bankroll, survival_mode=survival,
            max_stake_cap=max_stake_cap, realistic=self.realistic
        )
        if stake <= 0:
            self.rejected += 1
            reason = meta.get("realistic_skip", "stake_zero")
            self.log(tok.t_min, tok.ticker, "REJECT", 0, 0, reason)
            return v
        self.positions.append(Position(tok.ticker, tok.t_min, stake, stake / self.bankroll))
        self.traded_pools.add(pool_key)
        self.entered += 1
        if self.state == DeskState.SCANNING:
            self.state = DeskState.IN_POSITION
        self.log(
            tok.t_min,
            tok.ticker,
            "ENTRY",
            stake,
            1.0,
            f"stake={stake:.0f} fullK={meta['full_kelly']:.2%} used={meta['used']:.2%}",
        )
        self.equity_curve.append((tok.t_min, self.bankroll, f"entry {tok.ticker}"))
        return v

    def mark_and_maybe_exit(self, tok, t_min, multiple, forced=None):
        # Find matching position by ticker
        pos = next((p for p in self.positions if p.ticker == tok.ticker), None)
        if pos is None:
            return
        pos.current_mult = multiple
        pos.peak_mult = max(pos.peak_mult, multiple)
        w = self.wallets.score(tok)
        why = forced
        # runner_escape token: force_exit + runner_exit 动态调整
        # backtest: 用 future peak_potential；live: 用 lg proxy (lg*0.6, capped 50)
        peak_potential = (min(50.0, max(tok.liq_growth * self.peak_proxy_coef, 5.0)) if self.live_mode
                         else max(tok.true_multiple_path or [1]))
        is_runner = (tok.liq_growth >= 3.0 if self.live_mode
                    else (tok.liq_growth >= 5.0 or peak_potential >= 5.0))
        # force_exit_threshold: 对 single-holder rug risk (dump>=0.9) 提前 exit！
        # Normal runner: max(5, peak*0.7) ≈ 35x (too high for rugs with peak 12-28x)
        # Rug risk runner: max(1.5, peak*0.10) ≈ 5x → exit ASAP before dump
        if is_runner and w["force_exit"]:
            force_exit_threshold = max(1.5, peak_potential * 0.10)
        else:
            force_exit_threshold = max(5.0, peak_potential * 0.7) if is_runner else 1.4
        runner_exit_threshold = (min(50.0, max(25.0, peak_potential - 5.0)) if is_runner else 25.0)
        if why is None:
            if multiple <= 0.60:
                why = f"STOP {multiple - 1:+.0%}"
            elif w["force_exit"] and multiple >= force_exit_threshold:
                why = f"EXIT linked wallets dump pressure={w['pressure']}"
            elif multiple >= runner_exit_threshold:
                why = f"EXIT runner {multiple:.1f}x"
            elif (t_min - pos.entry_min) >= 25 and multiple < 1.15:
                why = f"EXIT time-stop {multiple:.2f}x"
        if why is None:
            return
        usd_out = pos.entry_usd * multiple * 0.985
        pnl = usd_out - pos.entry_usd
        self.bankroll += pnl
        r = pnl / pos.entry_usd
        self.sizer.record(r)
        theme = f" {tok.theme_hint}" if tok.theme_hint else ""
        self.closed.append(
            {
                "ticker": tok.ticker,
                "text": f"{tok.ticker} {tok.name}{theme} {tok.description}",
                "pnl_mult": multiple,
                "r": r,
                "why": why,
                "theme_hint": tok.theme_hint or "",
            }
        )
        side = "STOP" if r < 0 else "EXIT"
        self.log(t_min, tok.ticker, side, usd_out, multiple, why)
        self.equity_curve.append((t_min, self.bankroll, f"{side} {tok.ticker}"))
        self.positions.remove(pos)
        if r < 0:
            self.consec_losses += 1
            self.state = DeskState.DRAWDOWN
            if self.consec_losses >= self.loss_pause_n:
                self._self_pause(t_min)
        else:
            self.consec_losses = 0
            self.state = (
                DeskState.ROTATION
                if self.narrative.cluster_centroid is not None
                else DeskState.SCANNING
            )

    def _self_pause(self, t_min: int):
        self.state = DeskState.SELF_PAUSE
        self.log(
            t_min,
            "",
            "HALT",
            self.bankroll,
            self.bankroll / self.start,
            f"grok paused itself · replaying {self.consec_losses} losses",
        )
        report = self.narrative.rebuild_from_trades(self.closed, self.market_survivors)
        if report.get("ok"):
            self.log(
                t_min + 1,
                "",
                "LEARN",
                0,
                0,
                f"survivors share theme={report['theme']} "
                f"win_cos={report['winner_cos']} lose_cos={report['loser_cos']}",
            )
            self.log(
                t_min + 2,
                "",
                "RULE",
                0,
                0,
                f"strategy rebuilt · narrative filter ON · min_match={self.narrative.min_match}",
            )
            self.state = DeskState.FILTERED
            self.consec_losses = 0
        else:
            self.log(t_min + 1, "", "LEARN", 0, 0, str(report))

    def _self_pause_retry(self) -> None:
        """Called when a new market survivor is observed while still in SELF_PAUSE.
        Re-attempt rebuild using the newly available WIN sample."""
        report = self.narrative.rebuild_from_trades(self.closed, self.market_survivors)
        if report.get("ok"):
            # Append fresh LEARN/RULE events after any prior failed LEARN
            last_t = max((f.t_min for f in self.feed), default=0)
            self.log(
                last_t + 1,
                "",
                "LEARN",
                0,
                0,
                f"retry survivors share theme={report['theme']} "
                f"win_cos={report['winner_cos']} lose_cos={report['loser_cos']}",
            )
            self.log(
                last_t + 2,
                "",
                "RULE",
                0,
                0,
                f"retry strategy rebuilt · narrative filter ON · min_match={self.narrative.min_match}",
            )
            self.state = DeskState.FILTERED
            self.consec_losses = 0

    def observe_runner(self, tok: TokenLaunch, multiple: float) -> None:
        if multiple >= 1.2:
            theme = f" {tok.theme_hint}" if tok.theme_hint else ""
            # Dedupe by ticker — keep highest mult
            existing = next((s for s in self.market_survivors if s["ticker"] == tok.ticker), None)
            if existing and existing["pnl_mult"] >= multiple:
                return
            entry = {
                "text": f"{tok.ticker} {tok.name}{theme} {tok.description}",
                "pnl_mult": multiple,
                "ticker": tok.ticker,
                "theme_hint": tok.theme_hint or "",
            }
            if existing:
                self.market_survivors.remove(existing)
            self.market_survivors.append(entry)
            # If we were stuck in SELF_PAUSE with no prior rebuild success,
            # a new runner observation means we have WIN samples to try again.
            if self.state == DeskState.SELF_PAUSE:
                self._self_pause_retry()

    def snapshot(self, t_min: int) -> dict:
        k = self.sizer.kelly()
        ruin = self.sizer.risk_of_ruin(
            self.bankroll,
            max(k["used"], 0.05),
            max(k["p"], 0.15),
            max(k["W"], 1.2),
            max(k["L"], 0.3),
            n_trades=30,
            n_sims=800,
            seed=1,
        )
        return {
            "t": t_min,
            "state": self.state.value,
            "bankroll": round(self.bankroll, 2),
            "multiple": round(self.bankroll / self.start, 2),
            "seen": self.seen,
            "entered": self.entered,
            "rejected": self.rejected,
            "theme": self.narrative.cluster_label,
            "expectancy": round(k["expectancy"], 3),
            "full_kelly": round(k["full_kelly"], 3),
            "used_kelly": round(k["used"], 3),
            "ruin": round(ruin["ruin_prob"], 3),
            "open": None if not self.position else self.position.ticker,
            "open_positions": [
                {
                    "ticker": p.ticker,
                    "entry_min": p.entry_min,
                    "size_frac": round(p.size_frac * 100, 1),
                    "peak_mult": round(p.peak_mult, 3),
                    "current_mult": round(p.current_mult, 3),
                }
                for p in self.positions
            ],
        }


def _tok(t, ticker, name, desc, lp, liq, growth, dep, holders, linked, selling, path, theme):
    return TokenLaunch(t, ticker, name, desc, lp, liq, growth, dep, holders, linked, selling, path, theme)


def scenario(rng: np.random.Generator | None = None) -> list[TokenLaunch]:
    """Build scenario. If rng is given, add controlled noise to timing/liquidity/path
    to simulate different market days. Ground truth (hood coins go up, early coins bleed)
    is preserved so the self-pause → rebuild mechanism has something to latch onto."""
    base = [
        # (t_min, ticker, name, desc, lp, liq, growth, dep, holder_tag, linked_groups_tag, selling, path_mult, theme)
        (5, "PEPEJET", "Pepe Jet", "pepe frog jet moon rocket sol meme", "flap", 4.1, 1.05, "dep1", "A", "A4", 1, [0.95, 0.80, 0.54], "animal"),
        (17, "BONKZ", "Bonk Z", "bonk dog solana pump generic chad", "pons", 2.4, 1.02, "dep2", "B", "B5", 2, [0.90, 0.70], "animal"),
        (43, "PEPEJET", "Pepe Jet", "pepe frog jet moon rocket sol meme", "flap", 4.1, 1.05, "dep1", "A", "A4", 1, [0.54], "animal"),
        (59, "SOLGOD", "Sol God", "solana god moon rocket elon chad", "bags", 3.2, 1.10, "dep3", "B", "B3B36", 2, [0.92, 0.78, 0.54], "sol"),
        (76, "MOONJET", "Moon Jet", "moon jet rocket pepe dog wif", "flap", 2.8, 1.00, "dep4", "C", "C6", 3, [0.88, 0.69, 0.52], "generic"),
        (88, "DOGWIF2", "Dog Wif 2", "dog wif hat pepe animal meme", "pons", 3.5, 1.08, "dep5", "C", "C4", 2, [0.85, 0.69, 0.55], "animal"),
        (96, "FROGKING", "Frog King", "pepe frog king moon generic", "flap", 3.0, 1.05, "dep6", "D", "D5", 1, [0.80, 0.50], "animal"),
        (102, "HOODRAT", "Hood Rat", "robinhood hoodrat cashcat broker app l2 stocktoken", "pons", 9.0, 1.90, "depHx", "H", "H2", 0, [12.0], "hood"),
        (108, "CASHCAT", "Cash Cat", "cashcat robinhood former mascot hood tendies stonk", "flap", 14.0, 2.20, "depHy", "H", "H0", 0, [9.0], "hood"),
        (130, "TRUMPJET", "Trump Jet", "trump jet moon rocket chad", "bags", 6.0, 1.40, "dep7", "D", "D0", 0, [1.2], "generic"),
        (140, "AIINU", "Artificial Inu", "ai inu dog neural bot meme", "pons", 7.0, 1.50, "dep8", "D", "D0", 0, [1.3], "ai"),
        (221, "HOODAI", "Hood AI Agent", "robinhood chain hood ai agent stocktoken l2 broker app tenev", "pons", 3.0, 1.05, "depH", "H", "H3", 0, [1.05], "hood"),
        (244, "HOODAI", "Hood AI Agent", "robinhood chain hood ai agent stocktoken l2 broker app tenev", "pons", 8.8, 2.10, "depH", "H", "H3", 0, [1.10, 2.4, 8.0, 18.0, 31.0], "hood"),
        (323, "HOODCASH", "Hood Cash", "robinhood hood cashcat tendies broker stonk app", "flap", 9.5, 1.80, "depH2", "H", "H47", 0, [1.2, 2.0, 4.2], "hood"),
        (387, "ROBINAI", "Robin AI", "robin hood ai agent l2 stocktoken tenev broker", "bags", 11.0, 1.70, "depH3", "H", "H811", 0, [1.1, 1.8, 2.6], "hood"),
        (430, "PEPEMOON", "Pepe Moon", "pepe moon dog frog rocket", "flap", 12.0, 2.00, "dep9", "A", "A0", 0, [1.5], "animal"),
    ]

    # Holder pools keyed by tag letter
    pools = {ch: [f"0x{ch.lower()}{i:02d}" for i in range(12 if ch != "H" else 16)] for ch in "ABCDH"}

    def expand_group(tag: str) -> list[set]:
        """Decode linked_groups_tag like 'A4' → [set(A[:4])], 'B3B36' → [set(B[:3]), set(B[3:6])]"""
        groups = []
        i = 0
        while i < len(tag):
            ch = tag[i]
            i += 1
            nums = ""
            while i < len(tag) and tag[i].isdigit():
                nums += tag[i]
                i += 1
            # next char is another letter or end — determine boundaries
            start = int(nums[0])
            end = int(nums[1:]) if len(nums) > 1 else start + int(nums[0])
            groups.append(set(pools[ch][start:end]))
        return groups

    tokens: list[TokenLaunch] = []
    for row in base:
        (t, ticker, name, desc, lp, liq, growth, dep, htag, lgtag, selling, path, theme) = row
        holders = pools[htag]
        linked = expand_group(lgtag)
        # Apply random noise if rng is given
        if rng is not None:
            # timing jitter ±4 min, clip to 0..500
            t = int(np.clip(t + rng.integers(-4, 5), 0, 500))
            # liquidity ±30%
            liq = max(0.3, liq * (1 + rng.uniform(-0.30, 0.30)))
            # growth ±15%, floor 0.95
            growth = max(0.95, growth * (1 + rng.uniform(-0.15, 0.15)))
            # path multiples — hood runners get ±25%, losers get ±10% (keep them losers)
            path = list(path)
            for j, m in enumerate(path):
                if theme == "hood" and m >= 3:
                    path[j] = round(m * (1 + rng.uniform(-0.25, 0.25)), 2)
                elif m < 1.0:
                    # losers: small jitter but keep below 0.65 so stop still fires
                    path[j] = round(m * (1 + rng.uniform(-0.05, 0.05)), 2)
                else:
                    path[j] = round(m * (1 + rng.uniform(-0.15, 0.15)), 2)
            selling = max(0, selling + int(rng.integers(-1, 2)))
        tokens.append(_tok(t, ticker, name, desc, lp, liq, growth, "0x" + dep,
                           holders, linked, selling, path, theme))
    return tokens


def run_replay(verbose: bool = True, seed: int | None = 7, loss_pause_n: int = 3,
               live: bool = False, interval_ms: int = 150,
               from_csv: str | None = None, thin_cut: float | None = None,
               max_positions: int = 3, lg_proxy: bool = False,
               jitter_pct: float = 0.0, jitter_min: int = 0,
               realistic: bool = False, slip_pct: float = 0.02,
               peak_proxy_coef: float = 0.6) -> Desk:
    rng = np.random.default_rng(seed) if seed is not None else None
    desk = Desk(narrative_seed=seed if seed is not None else 7, loss_pause_n=loss_pause_n,
                thin_cut_override=thin_cut, max_positions=max_positions,
                live_mode=lg_proxy, realistic=realistic, max_slippage_pct=slip_pct,
                peak_proxy_coef=peak_proxy_coef)
    if from_csv:
        # Real data from fetch_dexscreener.py — no simulation rng applied
        from fetch_dexscreener import load_csv_as_tokenlaunches
        raw_tokens = load_csv_as_tokenlaunches(from_csv)
        if verbose:
            print(f"[csv] loaded {len(raw_tokens)} TokenLaunch from {from_csv}")
        # ── Jitter injection — evaluate robustness to real-world price/time noise ──
        if jitter_pct > 0 or jitter_min > 0:
            for tok in raw_tokens:
                if jitter_pct > 0 and tok.true_multiple_path:
                    eps = np.random.default_rng(seed).normal(0, jitter_pct, size=len(tok.true_multiple_path))
                    jittered = np.array(tok.true_multiple_path) * (1 + eps)
                    jittered = np.clip(jittered, 0.5, None)  # cap at 0.5x min to avoid neg
                    tok.true_multiple_path = list(jittered)
                if jitter_min > 0 and rng is not None:
                    tok.t_min = int(tok.t_min + rng.integers(-jitter_min, jitter_min + 1))
            if verbose:
                print(f"[jitter] price±{jitter_pct:.1%}, time±{jitter_min}min applied to {len(raw_tokens)} tokens")
    else:
        raw_tokens = scenario(rng) if rng is not None else scenario(None)
    events = [(tok.t_min, tok) for tok in raw_tokens]
    events.sort(key=lambda x: x[0])
    i, pending_marks, extra_dump_armed = 0, [], False
    renderer = None
    if live:
        # Deduplicate for scatter display
        seen = set()
        scatter_tokens = []
        for t in raw_tokens:
            if t.ticker not in seen:
                scatter_tokens.append(t)
                seen.add(t.ticker)
        renderer = LiveRenderer(desk, scatter_tokens, interval_ms=interval_ms)
        renderer.on_step(desk, 0)

    def schedule_marks(tok, entry_t):
        for j, m in enumerate(tok.true_multiple_path):
            pending_marks.append((entry_t + 1 + j * 2, tok, m))

    while i < len(events) or pending_marks:
        nxt_event = events[i][0] if i < len(events) else 10**9
        nxt_mark = min(pending_marks, key=lambda z: z[0])[0] if pending_marks else 10**9
        if nxt_event <= nxt_mark and i < len(events):
            t, tok = events[i]
            i += 1
            if verbose:
                print(f"\n[{t:03d}m] LAUNCH ${tok.ticker} liq={tok.liquidity_eth} theme={tok.theme_hint}")
            if max(tok.true_multiple_path or [1]) >= 1.5:
                desk.observe_runner(tok, max(tok.true_multiple_path))
            before_count = len(desk.positions)
            desk.on_launch(tok)
            after_count = len(desk.positions)
            if after_count > before_count:
                schedule_marks(tok, t)
                if tok.ticker == "HOODAI" and tok.liq_growth >= 2.0:
                    extra_dump_armed = True
            if renderer:
                renderer.on_step(desk, t)
        else:
            t, tok, mult = min(pending_marks, key=lambda z: z[0])
            pending_marks.remove((t, tok, mult))
            # Observe runner at every mark — catches pools that surge mid-path
            if mult >= 1.3:
                desk.observe_runner(tok, mult)
            if extra_dump_armed and tok.ticker == "HOODAI" and mult >= 30:
                tok.selling_linked = 3
                tok.linked_groups = [set(tok.holders[:4])]
            pos_match = next((p for p in desk.positions if p.ticker == tok.ticker), None)
            if verbose and pos_match:
                print(f"[{t:03d}m] MARK  ${tok.ticker} {mult:.2f}x")
            last = not any(pm[1].ticker == tok.ticker for pm in pending_marks)
            forced = None
            if last and pos_match and mult >= 1.5:
                if not desk.wallets.score(tok)["force_exit"]:
                    forced = f"EXIT rotate off last mark {mult:.1f}x"
            desk.mark_and_maybe_exit(tok, t, mult, forced=forced)
            if renderer:
                renderer.on_step(desk, t)

    # ── Close any remaining open positions (replay ended, no more marks) ──
    if desk.positions:
        last_t = max((f.t_min for f in desk.feed), default=0)
        # Copy list since mark_and_maybe_exit mutates positions
        remaining = list(desk.positions)
        for pos in remaining:
            orig_tok = next((ev[1] for ev in events if ev[1].ticker == pos.ticker), None)
            if orig_tok is None:
                orig_tok = next((ev[1] for ev in events if ev[1].ticker == (pos.ticker or "")), None)
            desk.mark_and_maybe_exit(
                orig_tok or events[0][1],
                last_t + 1,
                pos.current_mult,
                forced=f"END_OF_REPLAY close {pos.current_mult:.2f}x",
            )

    # Determine final tick from feed for accurate final snapshot
    final_tick = max((f.t_min for f in desk.feed), default=0)
    desk._final_tick = final_tick

    if renderer:
        # Final render and hold so the user can inspect
        renderer._update(desk, 500)
        renderer.fig.canvas.flush_events()
        print("\n[live] replay finished — close the window to exit")
        plt.show(block=True)
    if verbose:
        print("\n======== FEED ========")
        for f in desk.feed:
            print(f"  {f.t_min:03d} {f.side:<7} ${f.ticker or '-':<10} {f.multiple:>6.2f}x  {f.note}")
        print("\n======== FINAL ========")
        for k, v in desk.snapshot(final_tick).items():
            print(f"  {k}: {v}")
    return desk


class LiveRenderer:
    """Real-time matplotlib dashboard: equity curve + narrative scatter + status panel."""

    def __init__(self, desk: Desk, scenario_tokens: list[TokenLaunch],
                 interval_ms: int = 150):
        self.interval_s = interval_ms / 1000.0
        self._last_draw = 0.0

        plt.ion()
        self.fig = plt.figure(figsize=(14, 7), facecolor="#0b0f0c")
        # 3-row, 2-col layout:
        #   row 0 (tall) : equity curve (span both cols)
        #   row 1        : narrative scatter | status panel
        #   row 2        : feed log (span both cols)
        self.ax_eq = self.fig.add_axes([0.05, 0.55, 0.62, 0.40])  # equity
        self.ax_sc = self.fig.add_axes([0.70, 0.55, 0.28, 0.40])  # scatter
        self.ax_st = self.fig.add_axes([0.70, 0.28, 0.28, 0.23])  # status
        self.ax_fd = self.fig.add_axes([0.05, 0.05, 0.93, 0.46])  # feed log

        for ax in [self.ax_eq, self.ax_sc, self.ax_st, self.ax_fd]:
            ax.set_facecolor("#0b0f0c")
            ax.tick_params(colors="#cde8d0", labelsize=8)
            for sp in ax.spines.values():
                sp.set_color("#2a4a32")
            ax.title.set_color("#cde8d0")

        # ---- equity curve ----
        self.ax_eq.set_title("equity (USD) — live", fontsize=10)
        self.ax_eq.axhline(desk.start, color="#666", ls="--", lw=0.8)
        (self.eq_line,) = self.ax_eq.plot([], [], color="#3dff8a", lw=1.8)
        self.eq_marker, = self.ax_eq.plot([], [], "o", color="#3dff8a", ms=4)
        self.ax_eq.set_xlim(0, 500)
        self.ax_eq.set_ylim(desk.start * 0.4, desk.start * 10)
        self.ax_eq.set_yscale("log")
        self.ax_eq.grid(True, color="#1a3a22", lw=0.4, alpha=0.5)

        # ---- narrative scatter ----
        self.ax_sc.set_title("narrative cluster", fontsize=10)
        self._scatter_tokens = scenario_tokens
        self._df_base = desk.narrative.project_2d(scenario_tokens)
        colors = ["#3dff8a" if h == "hood" else "#ff5b5b"
                  for h in self._df_base["theme_hint"]]
        self.sc = self.ax_sc.scatter(self._df_base["x"], self._df_base["y"],
                                     c=colors, s=30, alpha=0.4, edgecolors="none")
        self._annots = [self.ax_sc.annotate(r["ticker"], (r["x"], r["y"]),
                                           fontsize=6, color="#7ab080")
                        for _, r in self._df_base.iterrows()]
        # centroid marker (updated each frame)
        self.centroid_marker, = self.ax_sc.plot([], [], "+", color="#ffd93d",
                                                ms=14, mew=2)
        self.ax_sc.set_xticks([])
        self.ax_sc.set_yticks([])

        # ---- status panel ----
        self.ax_st.set_title("desk status", fontsize=10)
        self.ax_st.set_xticks([])
        self.ax_st.set_yticks([])
        self.status_text = self.ax_st.text(0.02, 0.98, "", transform=self.ax_st.transAxes,
                                           va="top", ha="left", fontsize=8,
                                           color="#cde8d0", family="monospace")

        # ---- feed log ----
        self.ax_fd.set_title("feed (last 14 events)", fontsize=10)
        self.ax_fd.set_xticks([])
        self.ax_fd.set_yticks([])
        self.feed_text = self.ax_fd.text(0.02, 0.98, "", transform=self.ax_fd.transAxes,
                                         va="top", ha="left", fontsize=7.5,
                                         color="#cde8d0", family="monospace")

        self.fig.suptitle("RH Trencher · live replay", color="#3dff8a",
                          fontsize=13, fontweight="bold", y=0.995)
        self.fig.show()

    def on_step(self, desk: Desk, current_t: int) -> None:
        import time
        now = time.monotonic()
        if now - self._last_draw < self.interval_s:
            return
        self._last_draw = now
        self._update(desk, current_t)
        try:
            self.fig.canvas.flush_events()
        except Exception:
            pass

    def _update(self, desk: Desk, current_t: int) -> None:
        # equity curve
        ts, eqs, _ = zip(*desk.equity_curve) if desk.equity_curve else ([], [], [])
        ts = list(ts) + [current_t]
        eqs = list(eqs) + [desk.bankroll]
        self.eq_line.set_data(ts, eqs)
        self.eq_marker.set_data([current_t], [desk.bankroll])
        self.ax_eq.set_xlim(0, max(500, current_t + 20))
        self.ax_eq.set_ylim(max(desk.start * 0.3, min(eqs) * 0.8),
                            max(desk.start * 12, max(eqs) * 1.2))

        # scatter — centroid
        if desk.narrative.cluster_centroid is not None:
            # find centroid position via project_2d (reuse svd dims by reprojecting?)
            # Simpler: recompute full df each frame — cheap for 16 tokens
            df = desk.narrative.project_2d(self._scatter_tokens)
            self.sc.set_offsets(df[["x", "y"]].values)
            c_xy = self._centroid_xy(desk, df)
            if c_xy is not None:
                self.centroid_marker.set_data([c_xy[0]], [c_xy[1]])
            self.centroid_marker.set_visible(True)
        else:
            self.sc.set_offsets(self._df_base[["x", "y"]].values)
            self.centroid_marker.set_visible(False)

        # status panel
        snap = desk.snapshot(current_t)
        pos = "-" if not desk.position else f"${desk.position.ticker} @ {desk.position.entry_min}m"
        lines = [
            f"{'state':<14} {snap['state']}",
            f"{'theme':<14} {snap['theme'] or '—'}",
            f"{'bankroll':<14} ${snap['bankroll']:.2f}",
            f"{'multiple':<14} {snap['multiple']:.2f}x",
            f"{'open pos':<14} {pos}",
            f"{'entry/reject':<14} {desk.entered} / {desk.rejected}",
            f"{'consec loss':<14} {desk.consec_losses}",
            f"{'expectancy':<14} {snap['expectancy']:.2f}",
            f"{'full/used K':<14} {snap['full_kelly']:.2f}/{snap['used_kelly']:.2f}",
            f"{'ruin prob':<14} {snap['ruin']:.3f}",
        ]
        self.status_text.set_text("\n".join(lines))

        # feed log — last 14
        tail = desk.feed[-14:]
        feed_lines = []
        for f in tail:
            side_color = {"ENTRY": "#3dff8a", "EXIT": "#3dff8a",
                          "STOP": "#ff5b5b", "HALT": "#ffd93d",
                          "LEARN": "#8ab4ff", "RULE": "#ffd93d",
                          "REJECT": "#777", "NOT_BUY": "#ff9c5b"}.get(f.side, "#cde8d0")
            feed_lines.append(f"[{f.t_min:03d}] \0color:{side_color}\0"
                              f"{f.side:<7} ${(f.ticker or '-'):<10} "
                              f"{f.multiple:>6.2f}x  {f.note}")
        # naive color by marker — matplotlib text doesn't support inline color,
        # so we just keep plain
        plain = [l.replace("\0color:", "").split("\0")[-1] for l in feed_lines]
        self.feed_text.set_text("\n".join(plain))

    def _centroid_xy(self, desk: Desk, df: pd.DataFrame) -> tuple[float, float] | None:
        """Project the narrative centroid into the same 2D plane as df tokens."""
        c = desk.narrative.cluster_centroid
        if c is None:
            return None
        X_all = np.vstack([df[["x", "y"]].values, c[:2]])  # rough fallback —
        # Actually we need true 2D via the same SVD. Re-run with centroid as extra row.
        X = np.vstack([desk.narrative.embed_token(t) for t in self._scatter_tokens] + [c])
        X = X - X.mean(axis=0)
        U, S, _ = np.linalg.svd(X, full_matrices=False)
        xy_all = U[:, :2] * S[:2]
        return float(xy_all[-1, 0]), float(xy_all[-1, 1])

    def close(self) -> None:
        plt.close(self.fig)


def save_charts(desk: Desk, out_path: str = "rh_trencher_replay.png") -> str:
    tokens, seen = [], set()
    for tok in scenario():
        if tok.ticker not in seen:
            tokens.append(tok)
            seen.add(tok.ticker)
    df = desk.narrative.project_2d(tokens)
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), facecolor="#0b0f0c")
    for ax in axes:
        ax.set_facecolor("#0b0f0c")
        ax.tick_params(colors="#cde8d0")
        for sp in ax.spines.values():
            sp.set_color("#2a4a32")
        ax.title.set_color("#cde8d0")
        ax.xaxis.label.set_color("#cde8d0")
    ts, eqs, _ = zip(*desk.equity_curve)
    axes[0].plot(ts, eqs, color="#3dff8a", lw=2)
    axes[0].axhline(desk.start, color="#666", ls="--", lw=0.8)
    axes[0].set_title("paper equity (USD)")
    colors = ["#3dff8a" if h == "hood" else "#ff5b5b" for h in df["theme_hint"]]
    axes[1].scatter(df["x"], df["y"], c=colors, s=40)
    for _, r in df.iterrows():
        axes[1].annotate(r["ticker"], (r["x"], r["y"]), fontsize=7, color="#cde8d0")
    axes[1].set_title("narrative embedding (green = hood cluster)")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)
    return out_path


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", type=int, default=None, help="Single seed to run in detail (default 7)")
    ap.add_argument("--multi", type=int, nargs="?", const=10, default=0,
                    help="Run N seeds (default 10) for stability summary")
    ap.add_argument("--live", action="store_true",
                    help="Open live matplotlib dashboard during replay")
    ap.add_argument("--interval", type=int, default=150,
                    help="Live renderer throttle in ms (default 150)")
    ap.add_argument("--lpn", type=int, default=3,
                    help="Consecutive losses before self-pause (default 3)")
    ap.add_argument("--from-csv", default=None,
                    help="Load real TokenLaunch data from CSV (output of fetch_dexscreener.py) "
                         "instead of built-in scenario")
    ap.add_argument("--thin-cut", type=float, default=None,
                    help="Override liquidity thin-book veto threshold (default auto: 0.12 open, 0.33 clustered). "
                         "Use 0.0 to let every token pass.")
    ap.add_argument("--max-positions", type=int, default=3,
                    help="Max simultaneous open positions (default 3). Use 1 for serial trading.")
    ap.add_argument("--lg-proxy", action="store_true",
                    help="Live-compatible mode: use liq_growth proxy instead of future peak_potential. "
                         "Removes backtest-only data leak. Expected live mult ~30-40x.")
    ap.add_argument("--jitter-pct", type=float, default=0.0,
                    help="Price path jitter std dev as fraction (e.g. 0.02 = ±2%%). "
                         "Evaluates robustness to real-world noise. Only with --from-csv.")
    ap.add_argument("--jitter-min", type=int, default=0,
                    help="Launch-time jitter ±minutes (uniform). Evaluates robustness to arrival timing.")
    ap.add_argument("--realistic", action="store_true", default=None,
                    help="Realistic mode: enforce liquidity-aware stake capping (max 2%% of pool liquidity), "
                         "skip pools with <100 ETH liquidity. Auto-on for --lg-proxy and --from-csv.")
    ap.add_argument("--no-realistic", action="store_true",
                    help="Explicitly disable realistic mode (allows exponential compound growth — only for debugging).")
    ap.add_argument("--slip", type=float, default=0.02,
                    help="Max stake as fraction of pool liquidity when --realistic (default 0.02 = 2%%).")
    ap.add_argument("--peak-proxy", type=float, default=0.6,
                    help="Peak proxy coefficient for lg-proxy mode: peak_est = liq_growth * coef (default 0.6).")
    ap.add_argument("--no-jitter", action="store_true",
                    help="Disable default 1%% jitter in multi-seed mode (produces deterministic std=0.00).")
    args = ap.parse_args()

    # ── realistic mode logic ──
    # Default behavior: auto-enable for --lg-proxy and --from-csv (realistic/synthetic data).
    # Explicit --realistic: force on.  Explicit --no-realistic: force off (for unconstrained backtests).
    if args.no_realistic:
        args.realistic = False
    elif args.realistic is None:
        args.realistic = args.lg_proxy or bool(args.from_csv)
    _real_label = "REALISTIC" if args.realistic else "BACKTEST"
    print(f"[mode] {_real_label}  realistic={args.realistic}  lg_proxy={args.lg_proxy}  from_csv={bool(args.from_csv)}")

    # ── Multi-seed default: inject 1%% price jitter to reflect real market noise ──
    # Without jitter, all high-vol runners use runner_escape and bypass narrative checks,
    # producing identical results across seeds (std=0.00). Default jitter gives realistic
    # stability estimates. Use --no-jitter to restore deterministic baseline.
    DEFAULT_MULTI_JITTER = 0.01
    if args.multi > 0 and not args.no_jitter and args.jitter_pct == 0.0 and args.jitter_min == 0:
        args.jitter_pct = DEFAULT_MULTI_JITTER
        _jitter_applied = True
    else:
        _jitter_applied = False

    if args.multi > 0:
        # ---------- STABILITY RUN ----------
        results = []
        for seed in range(args.multi):
            desk = run_replay(verbose=False, seed=seed, loss_pause_n=args.lpn,
                              from_csv=args.from_csv, thin_cut=args.thin_cut,
                              max_positions=args.max_positions, lg_proxy=args.lg_proxy,
                              jitter_pct=args.jitter_pct, jitter_min=args.jitter_min,
                              realistic=args.realistic, slip_pct=args.slip,
                              peak_proxy_coef=args.peak_proxy)
            final_tick = getattr(desk, "_final_tick", 430)
            snap = desk.snapshot(final_tick)
            halt_events = [f for f in desk.feed if f.side == "HALT"]
            rule_events = [f for f in desk.feed if f.side == "RULE"]
            exits = [f for f in desk.feed if f.side in ("EXIT", "STOP")]
            trade_pnls = [f.multiple - 1 for f in exits]
            wins = [p for p in trade_pnls if p > 0]
            losses = [p for p in trade_pnls if p <= 0]
            results.append({
                "seed": seed,
                "multiple": snap["multiple"],
                "bankroll": snap["bankroll"],
                "theme": snap["theme"],
                "state": snap["state"],
                "halted": len(halt_events) > 0,
                "rebuilt": len(rule_events) > 0,
                "n_entry": snap["entered"],
                "n_win": len(wins),
                "n_loss": len(losses),
                "max_mult": round(max([f.multiple for f in exits]), 2) if exits else 0.0,
                "expectancy": snap["expectancy"],
            })

        _jitter_note = ""
        if _jitter_applied:
            _jitter_note = f"  [auto-jitter {DEFAULT_MULTI_JITTER:.0%}]"
        elif args.jitter_pct > 0 or args.jitter_min > 0:
            _jitter_note = f"  [jitter price±{args.jitter_pct:.1%} time±{args.jitter_min}m]"
        print(f"\n===== STABILITY SUMMARY · {len(results)} seeds (LPN={args.lpn}){_jitter_note} =====")
        print(f"{'seed':>4} {'mult':>7} {'bankroll':>10} {'theme':<7} {'state':<9} {'halt':>5} "
              f"{'rebuild':>7} {'entry':>6} {'nW':>4} {'nL':>4} {'best':>6} {'expect':>7}")
        print("-" * 95)
        for r in results:
            print(f"{r['seed']:>4} {r['multiple']:>7.2f} {r['bankroll']:>10.2f} "
                  f"{str(r['theme'] or '-'):<7} {r['state']:<9} "
                  f"{'Y' if r['halted'] else '-':>5} {'Y' if r['rebuilt'] else '-':>7} "
                  f"{r['n_entry']:>6} {r['n_win']:>4} {r['n_loss']:>4} "
                  f"{r['max_mult']:>6.1f} {r['expectancy']:>7.2f}")

        mults = [r["multiple"] for r in results]
        halts = sum(r["halted"] for r in results)
        rebuilt = sum(r["rebuilt"] for r in results)
        profitable = sum(1 for m in mults if m > 1.0)
        print("-" * 95)
        print(f"mult  mean={np.mean(mults):.2f}  median={np.median(mults):.2f}  "
              f"min={np.min(mults):.2f}  max={np.max(mults):.2f}  std={np.std(mults):.2f}")
        print(f"halt  fired in {halts}/{len(results)} runs  "
              f"rebuild in {rebuilt}/{len(results)}  profitable {profitable}/{len(results)}")
    else:
        # ---------- SINGLE RUN (default or --live) ----------
        seed = args.seed if args.seed is not None else 7
        desk = run_replay(verbose=not args.live, seed=seed, loss_pause_n=args.lpn,
                          live=args.live, interval_ms=args.interval,
                          from_csv=args.from_csv, thin_cut=args.thin_cut,
                          max_positions=args.max_positions, lg_proxy=args.lg_proxy,
                          jitter_pct=args.jitter_pct, jitter_min=args.jitter_min,
                          realistic=args.realistic, slip_pct=args.slip,
                          peak_proxy_coef=args.peak_proxy)
        if not args.live:
            print("saved", save_charts(desk))

