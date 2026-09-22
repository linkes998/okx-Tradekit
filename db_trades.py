#!/usr/bin/env python3
"""SQLite-based persistent trade storage for DeskRunner.

Multi-tenant layout
-------------------
``user_id = 0``  → system default / shared desk data (the data the site shows
                   before a member configures their own trade settings + API key).
``user_id = N``  → data belonging to member N. Dashboard shows only this slice
                   once that member has saved a complete set of API credentials.
"""
from __future__ import annotations
import hashlib
import secrets
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

#: Rows owned by the shared/system desk (no member context).
SYSTEM_UID = 0

#: Canonical default notional (USD) for one auto-trade entry. This is the single
#: source of truth shared by the DB column default, the settings UI and the
#: execution path — so the default, a member's saved value and what the trader
#: actually spends can never drift apart.
DEFAULT_TRADE_USD = 50.0


class TradeDB:
    """Thread-safe SQLite store for closed trades and open positions."""

    #: How long a session validation result may be reused before re-querying.
    SESSION_CACHE_TTL = 5.0

    def __init__(self, db_path: str = "trades.db"):
        self._path = Path(db_path)
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        # token → (verified_at, info|None). Avoids a DB round-trip on every
        # 1-second dashboard poll while still honouring logout/expiry.
        self._sess_cache: dict[str, tuple[float, dict | None]] = {}
        self._init_db()
        self._init_members()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self._path), check_same_thread=False, timeout=15.0
            )
            conn = self._conn
            # ── Performance tuning: this DB is written by the runner thread and
            # read by many HTTP worker threads at 1 Hz per client.
            conn.execute("PRAGMA journal_mode=WAL")        # concurrent readers + 1 writer
            conn.execute("PRAGMA synchronous=NORMAL")      # WAL + NORMAL is crash-safe
            conn.execute("PRAGMA cache_size=-16000")       # ~16 MB page cache
            conn.execute("PRAGMA temp_store=MEMORY")       # keep sorts/aggregates off disk
            conn.execute("PRAGMA mmap_size=268435456")     # 256 MB memory-mapped I/O
            conn.execute("PRAGMA busy_timeout=15000")      # avoid SQLITE_BUSY under load
        return self._conn

    # ── schema helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        except sqlite3.Error:
            return set()
        return {r[1] for r in rows}

    @staticmethod
    def _ensure_column(conn: sqlite3.Connection, table: str, col: str, decl: str) -> None:
        """Idempotent ADD COLUMN (SQLite has no ADD COLUMN IF NOT EXISTS)."""
        if col not in TradeDB._columns(conn, table):
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS closed_trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker      TEXT NOT NULL,
                side        TEXT NOT NULL,
                entry_usd   REAL NOT NULL,
                exit_usd    REAL NOT NULL,
                pnl_usd     REAL NOT NULL,
                pnl_mult    REAL,
                win         INTEGER NOT NULL,
                entry_min   INTEGER,
                exit_min    INTEGER,
                entry_ts    REAL,
                exit_ts     REAL,
                why         TEXT DEFAULT '',
                platform    TEXT DEFAULT 'JUP',
                order_id    TEXT DEFAULT '',
                user_id     INTEGER NOT NULL DEFAULT 0,
                created_at  REAL NOT NULL
            )
        """)
        # Legacy DBs: add the tenancy column in place.
        self._ensure_column(conn, "closed_trades", "user_id", "INTEGER NOT NULL DEFAULT 0")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS open_trades (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker      TEXT NOT NULL,
                side        TEXT NOT NULL DEFAULT 'BUY',
                entry_usd   REAL NOT NULL,
                entry_min   INTEGER,
                entry_ts    REAL,
                platform    TEXT DEFAULT 'JUP',
                order_id    TEXT DEFAULT '',
                user_id     INTEGER NOT NULL DEFAULT 0,
                created_at  REAL NOT NULL,
                UNIQUE(ticker, user_id)
            )
        """)
        # Legacy `open_trades` had UNIQUE(ticker) at column level, which blocks
        # two members from holding the same ticker. Rebuild once with the
        # composite constraint, preserving all existing rows as system data.
        if "user_id" not in self._columns(conn, "open_trades"):
            conn.execute("ALTER TABLE open_trades RENAME TO open_trades_legacy")
            conn.execute("""
                CREATE TABLE open_trades (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker      TEXT NOT NULL,
                    side        TEXT NOT NULL DEFAULT 'BUY',
                    entry_usd   REAL NOT NULL,
                    entry_min   INTEGER,
                    entry_ts    REAL,
                    platform    TEXT DEFAULT 'JUP',
                    order_id    TEXT DEFAULT '',
                    user_id     INTEGER NOT NULL DEFAULT 0,
                    created_at  REAL NOT NULL,
                    UNIQUE(ticker, user_id)
                )
            """)
            conn.execute("""
                INSERT INTO open_trades
                    (id, ticker, side, entry_usd, entry_min, entry_ts, platform, order_id, user_id, created_at)
                SELECT id, ticker, side, entry_usd, entry_min, entry_ts, platform, order_id, 0, created_at
                FROM open_trades_legacy
            """)
            conn.execute("DROP TABLE open_trades_legacy")
            print("[TradeDB] migrated open_trades → composite UNIQUE(ticker, user_id)")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key     TEXT PRIMARY KEY,
                value   TEXT NOT NULL,
                updated_at  REAL NOT NULL
            )
        """)
        # ── Indexes tuned for the hot read paths ──
        conn.execute("CREATE INDEX IF NOT EXISTS idx_closed_ticker ON closed_trades(ticker)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_closed_ts ON closed_trades(exit_ts)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_closed_user_id ON closed_trades(user_id, id DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_closed_user_win ON closed_trades(user_id, win)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_open_user ON open_trades(user_id)")
        conn.commit()

    # ── closed trades ──────────────────────────────────────────────────────

    def add_trade(self, trade: dict) -> int:
        with self._lock:
            conn = self._get_conn()
            now = time.time()
            cur = conn.execute(
                """INSERT INTO closed_trades
                   (ticker, side, entry_usd, exit_usd, pnl_usd, pnl_mult, win,
                    entry_min, exit_min, entry_ts, exit_ts, why, platform, order_id,
                    user_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    trade.get("ticker", ""),
                    trade.get("side", "EXIT"),
                    float(trade.get("entry_usd", 0)),
                    float(trade.get("exit_usd", 0)),
                    float(trade.get("pnl_usd", 0)),
                    trade.get("pnl_mult"),
                    1 if trade.get("win", False) else 0,
                    trade.get("entry_min"),
                    trade.get("exit_min"),
                    trade.get("entry_ts"),
                    trade.get("exit_ts"),
                    trade.get("why", ""),
                    trade.get("platform", "JUP"),
                    trade.get("order_id", ""),
                    int(trade.get("user_id", SYSTEM_UID) or 0),
                    now,
                ),
            )
            conn.commit()
            return cur.lastrowid

    def get_recent(self, limit: int = 50, offset: int = 0,
                   user_id: int | None = SYSTEM_UID) -> list[dict]:
        where, params = self._scope_clause(user_id)
        with self._lock:
            conn = self._get_conn()
            rows = conn.execute(
                f"""SELECT id, ticker, side, entry_usd, exit_usd, pnl_usd, pnl_mult,
                          win, entry_min, exit_min, entry_ts, exit_ts, why, platform
                   FROM closed_trades
                   {where}
                   ORDER BY id DESC
                   LIMIT ? OFFSET ?""",
                (*params, limit, offset),
            ).fetchall()
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "side": r[2],
                "entry_usd": round(r[3], 2),
                "exit_usd": round(r[4], 2),
                "pnl_usd": round(r[5], 2),
                "pnl_mult": r[6],
                "win": bool(r[7]),
                "entry_min": r[8],
                "exit_min": r[9],
                "entry_ts": r[10],
                "exit_ts": r[11],
                "why": r[12],
                "platform": r[13],
            }
            for r in rows
        ]

    def get_total_count(self, user_id: int | None = SYSTEM_UID) -> int:
        where, params = self._scope_clause(user_id)
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                f"SELECT COUNT(*) FROM closed_trades {where}", params
            ).fetchone()
            return row[0] if row else 0

    def get_stats(self, user_id: int | None = SYSTEM_UID) -> dict:
        """Aggregate performance metrics for one tenant (or all when None).

        Everything is computed in a single aggregate pass; the consecutive
        win/loss streaks reuse the same query with a bounded LIMIT so the cost
        stays flat as history grows.
        """
        where, params = self._scope_clause(user_id)
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                f"""SELECT
                        COUNT(*)                                        AS total,
                        SUM(CASE WHEN win=1 THEN 1 ELSE 0 END)          AS wins,
                        SUM(CASE WHEN win=0 THEN 1 ELSE 0 END)          AS losses,
                        SUM(pnl_usd)                                    AS total_pnl,
                        AVG(pnl_usd)                                    AS avg_pnl,
                        SUM(CASE WHEN win=1 THEN pnl_usd ELSE 0 END)    AS gross_profit,
                        SUM(CASE WHEN win=0 THEN pnl_usd ELSE 0 END)    AS gross_loss,
                        AVG(CASE WHEN win=1 THEN pnl_usd END)           AS avg_win,
                        AVG(CASE WHEN win=0 THEN pnl_usd END)           AS avg_loss
                    FROM closed_trades {where}""",
                params,
            ).fetchone()
            streak_rows = conn.execute(
                f"SELECT win FROM closed_trades {where} ORDER BY id DESC LIMIT 500",
                params,
            ).fetchall()

        total, wins, losses, total_pnl, avg_pnl, gross_profit, gross_loss, avg_win, avg_loss = row
        max_cw = max_cl = cur_w = cur_l = 0
        for (w,) in streak_rows:
            if w:
                cur_w += 1
                cur_l = 0
                max_cw = max(max_cw, cur_w)
            else:
                cur_l += 1
                cur_w = 0
                max_cl = max(max_cl, cur_l)
        return {
            "total": total or 0,
            "total_trades": total or 0,
            "wins": wins or 0,
            "losses": losses or 0,
            "profit_rate": round((wins or 0) / (total or 1), 3),
            "win_rate": round((wins or 0) / (total or 1), 3),
            "total_pnl": round(total_pnl or 0, 2),
            "avg_pnl": round(avg_pnl or 0, 2),
            "avg_win": round(avg_win or 0, 2),
            "avg_loss": round(avg_loss or 0, 2),
            "gross_profit": round(gross_profit or 0, 2),
            "gross_loss": round(gross_loss or 0, 2),
            "max_consec_wins": max_cw,
            "max_consec_losses": max_cl,
        }

    def clear_all(self, user_id: int | None = SYSTEM_UID) -> int:
        where, params = self._scope_clause(user_id)
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                f"SELECT COUNT(*) FROM closed_trades {where}", params
            ).fetchone()
            conn.execute(f"DELETE FROM closed_trades {where}", params)
            conn.commit()
            return row[0] if row else 0

    # ── open trades ────────────────────────────────────────────────────────

    def add_open_trade(self, trade: dict) -> int:
        with self._lock:
            conn = self._get_conn()
            now = time.time()
            cur = conn.execute(
                """INSERT OR REPLACE INTO open_trades
                   (ticker, side, entry_usd, entry_min, entry_ts, platform, order_id,
                    user_id, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    trade.get("ticker", ""),
                    trade.get("side", "BUY"),
                    float(trade.get("entry_usd", 0)),
                    trade.get("entry_min"),
                    trade.get("entry_ts"),
                    trade.get("platform", "JUP"),
                    trade.get("order_id", ""),
                    int(trade.get("user_id", SYSTEM_UID) or 0),
                    now,
                ),
            )
            conn.commit()
            return cur.lastrowid

    def get_open_trades(self, user_id: int | None = SYSTEM_UID) -> list[dict]:
        where, params = self._scope_clause(user_id)
        with self._lock:
            conn = self._get_conn()
            rows = conn.execute(
                f"""SELECT ticker, side, entry_usd, entry_min, entry_ts, platform, order_id
                    FROM open_trades {where}""",
                params,
            ).fetchall()
        return [
            {
                "ticker": r[0],
                "side": r[1],
                "entry_usd": round(r[2], 2),
                "entry_min": r[3],
                "entry_ts": r[4],
                "platform": r[5],
                "order_id": r[6],
            }
            for r in rows
        ]

    def remove_open_trade(self, ticker: str, user_id: int | None = SYSTEM_UID) -> bool:
        with self._lock:
            conn = self._get_conn()
            if user_id is None:
                cur = conn.execute("DELETE FROM open_trades WHERE ticker=?", (ticker,))
            else:
                cur = conn.execute(
                    "DELETE FROM open_trades WHERE ticker=? AND user_id=?",
                    (ticker, int(user_id)),
                )
            conn.commit()
            return cur.rowcount > 0

    @staticmethod
    def _scope_clause(user_id: int | None) -> tuple[str, tuple]:
        """Build the tenancy WHERE clause. ``None`` = all tenants."""
        if user_id is None:
            return "", ()
        return "WHERE user_id=?", (int(user_id),)

    # ── settings (API keys, perp configs) ──────────────────────────────────

    def set_setting(self, key: str, value: str) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                """INSERT INTO settings (key, value, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at""",
                (key, value, time.time()),
            )
            conn.commit()

    def get_setting(self, key: str, default: str = "") -> str:
        with self._lock:
            conn = self._get_conn()
            row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def get_all_settings(self) -> dict[str, str]:
        with self._lock:
            conn = self._get_conn()
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r[0]: r[1] for r in rows}

    # ── password hashing (scrypt, with SHA-256 legacy migration) ───────────

    _SCRYPT_N = 16384
    _SCRYPT_R = 8
    _SCRYPT_P = 1
    _SCRYPT_DKLEN = 64

    def _hash_password(self, salt: str, password: str) -> str:
        """Hash a password with scrypt; return base64(salt||hash).

        The *salt* argument must be exactly 16 ASCII hex chars (32 hex digits
        from token_hex would be 32 bytes — we expect 16-byte salts).
        """
        salt_bytes = salt.encode("utf-8")
        dk = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt_bytes,
            n=self._SCRYPT_N,
            r=self._SCRYPT_R,
            p=self._SCRYPT_P,
            dklen=self._SCRYPT_DKLEN,
        )
        import base64
        # Store as base64(salt_bytes || dk_bytes)
        return base64.b64encode(salt_bytes + dk).decode("ascii")

    @classmethod
    def _hash_password_static(cls, salt: str, password: str) -> str:
        """Static version for use from @classmethod methods."""
        salt_bytes = salt.encode("utf-8")
        dk = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt_bytes,
            n=cls._SCRYPT_N,
            r=cls._SCRYPT_R,
            p=cls._SCRYPT_P,
            dklen=cls._SCRYPT_DKLEN,
        )
        import base64
        return base64.b64encode(salt_bytes + dk).decode("ascii")

    @classmethod
    def _verify_password(cls, stored: str, password: str, salt: str = "", migrate: bool = False):
        """Verify a password against a stored scrypt hash.

        Returns (ok: bool, new_hash_or_None: str | None).
        * When the stored value is a valid scrypt hash the result is
          (True, None) on success or (False, None) on failure.
        * When *migrate* is True and the stored value looks like a legacy
          SHA-256 hex digest (length < 80 bytes after base64 decode), the
          method re-hashes with scrypt using the supplied *salt* and returns
          (True, new_scrypt_hash) so the caller can atomically upgrade the
          row.
        """
        import base64
        try:
            raw = base64.b64decode(stored.encode("ascii"))
        except Exception:
            return False, None

        # Distinguish scrypt from legacy SHA-256 output.
        # A valid scrypt encoding is at least 16 bytes (salt) + 64 bytes (dk) = 80.
        if len(raw) < 80:
            # Legacy SHA-256 hex digest — attempt migration if salt provided.
            if migrate and salt:
                new_hash = cls._hash_password_static(salt, password)
                return True, new_hash
            return False, None

        # Use the DB-stored salt to extract dk from the raw hash.
        # raw = salt_bytes_from_hash_creation || dk_bytes
        # The salt stored in DB was used to create the hash, so find its position.
        if salt:
            salt_bytes = salt.encode("utf-8")
            # Locate salt_bytes in raw to split correctly
            idx = raw.find(salt_bytes)
            if idx < 0:
                return False, None
            expected_dk = raw[idx + len(salt_bytes):]
        else:
            # Fallback: assume salt is first 16 bytes
            salt_bytes = raw[:16]
            expected_dk = raw[16:]
        computed_dk = hashlib.scrypt(
            password.encode("utf-8"),
            salt=salt_bytes,
            n=cls._SCRYPT_N,
            r=cls._SCRYPT_R,
            p=cls._SCRYPT_P,
            dklen=len(expected_dk),
        )
        import hmac
        if not hmac.compare_digest(expected_dk, computed_dk):
            return False, None
        return True, None

    # ── members / auth ─────────────────────────────────────────────

    def _init_members(self) -> None:
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS members (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                username    TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt        TEXT NOT NULL,
                email       TEXT DEFAULT '',
                role        TEXT DEFAULT 'user',
                tier        TEXT DEFAULT 'free',
                created_at  REAL NOT NULL,
                last_login  REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                token       TEXT UNIQUE NOT NULL,
                created_at  REAL NOT NULL,
                expires_at  REAL NOT NULL,
                ip_address  TEXT DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leaderboard (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                total_pnl   REAL NOT NULL,
                trade_count INTEGER DEFAULT 0,
                win_rate    REAL DEFAULT 0,
                updated_at  REAL NOT NULL,
                UNIQUE(user_id)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_leaderboard_pnl ON leaderboard(total_pnl DESC)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER UNIQUE NOT NULL,
                api_key         TEXT DEFAULT '',
                api_secret      TEXT DEFAULT '',
                api_passphrase  TEXT DEFAULT '',
                risk_preference TEXT DEFAULT 'balanced',
                trade_mode      TEXT DEFAULT 'signal_only',
                run_start_time  TEXT,
                run_end_time    TEXT,
                max_position_usd REAL DEFAULT 100,
                min_trade_usd   REAL DEFAULT 10,
                max_trade_usd   REAL DEFAULT 100,
                trade_usd       REAL DEFAULT 50,
                allowed_tickers TEXT DEFAULT '',
                take_profit_pct REAL DEFAULT 3.0,
                stop_loss_pct   REAL DEFAULT 1.5,
                updated_at      REAL NOT NULL,
                FOREIGN KEY (user_id) REFERENCES members(id) ON DELETE CASCADE
            )
        """)
        # Legacy DBs: add the per-trade amount band in place.
        self._ensure_column(conn, "user_settings", "min_trade_usd", "REAL DEFAULT 10")
        self._ensure_column(conn, "user_settings", "max_trade_usd", "REAL DEFAULT 100")
        # Per-trade auto-execution notional (member-customisable, default $50).
        self._ensure_column(conn, "user_settings", "trade_usd", "REAL DEFAULT 50")
        conn.commit()
        self._ensure_admin()

    #: Column list shared by the user_settings read/write paths.
    USER_SETTINGS_COLUMNS = (
        "api_key", "api_secret", "api_passphrase",
        "risk_preference", "trade_mode", "run_start_time", "run_end_time",
        "max_position_usd", "min_trade_usd", "max_trade_usd", "trade_usd",
        "allowed_tickers", "take_profit_pct", "stop_loss_pct",
    )

    #: Defaults applied when a member saves a partial payload.
    USER_SETTINGS_DEFAULTS: dict[str, Any] = {
        "api_key": "", "api_secret": "", "api_passphrase": "",
        "risk_preference": "balanced", "trade_mode": "signal_only",
        "run_start_time": "00:00", "run_end_time": "23:59",
        "max_position_usd": 100, "min_trade_usd": 10, "max_trade_usd": 100,
        "trade_usd": DEFAULT_TRADE_USD,
        "allowed_tickers": "", "take_profit_pct": 3.0, "stop_loss_pct": 1.5,
    }


    def _ensure_admin(self) -> None:
        conn = self._get_conn()
        admin = conn.execute("SELECT id, password_hash FROM members WHERE username=?", ("admin",)).fetchone()
        if not admin:
            salt = secrets.token_hex(16)
            ph = self._hash_password(salt, "admin123")
            now = time.time()
            conn.execute(
                "INSERT INTO members (username, password_hash, salt, role, tier, created_at, last_login) VALUES (?,?,?,?,?,?,?)",
                ("admin", ph, salt, "admin", "premium", now, now),
            )
            conn.execute(
                "INSERT OR IGNORE INTO leaderboard (user_id, total_pnl, trade_count, win_rate, updated_at) VALUES (?,?,?,?,?)",
                (1, 0.0, 0, 0.0, now),
            )
            conn.commit()

    def create_user_settings_for_user(self, user_id: int) -> dict:
        """Auto-create a default user_settings row for a newly registered user."""
        with self._lock:
            conn = self._get_conn()
            existing = conn.execute(
                "SELECT id FROM user_settings WHERE user_id=?", (user_id,)
            ).fetchone()
            if existing:
                return {"ok": True, "created": False}
            self._insert_user_settings(conn, user_id, dict(self.USER_SETTINGS_DEFAULTS))
            conn.commit()
            return {"ok": True, "created": True}

    def _insert_user_settings(self, conn: sqlite3.Connection, user_id: int,
                              settings: dict) -> None:
        """INSERT a fresh user_settings row. Caller must hold self._lock."""
        cols = ", ".join(self.USER_SETTINGS_COLUMNS)
        marks = ", ".join("?" for _ in self.USER_SETTINGS_COLUMNS)
        vals = tuple(settings.get(c, self.USER_SETTINGS_DEFAULTS.get(c))
                     for c in self.USER_SETTINGS_COLUMNS)
        conn.execute(
            f"INSERT INTO user_settings (user_id, {cols}, updated_at) "
            f"VALUES (?, {marks}, ?)",
            (user_id, *vals, time.time()),
        )

    def register(self, username: str, password: str, email: str = "", role: str = "user") -> dict:
        with self._lock:
            conn = self._get_conn()
            existing = conn.execute("SELECT id FROM members WHERE username=?", (username,)).fetchone()
            if existing:
                return {"ok": False, "error": "username taken"}
            salt = secrets.token_hex(16)
            ph = self._hash_password(salt, password)
            now = time.time()
            cur = conn.execute(
                "INSERT INTO members (username, password_hash, salt, email, role, tier, created_at) VALUES (?,?,?,?,?,?,?)",
                (username, ph, salt, email, role, "free", now),
            )
            uid = cur.lastrowid
            conn.execute(
                "INSERT INTO leaderboard (user_id, total_pnl, trade_count, win_rate, updated_at) VALUES (?,?,?,?,?)",
                (uid, 0.0, 0, 0.0, now),
            )
            # Auto-create default user_settings for the new user
            self._insert_user_settings(conn, uid, dict(self.USER_SETTINGS_DEFAULTS))
            conn.commit()
            return {"ok": True, "user_id": uid, "username": username}

    def login(self, username: str, password: str) -> dict:
        with self._lock:
            conn = self._get_conn()
            row = conn.execute("SELECT id, password_hash, salt, role, tier FROM members WHERE username=?", (username,)).fetchone()
            if not row:
                return {"ok": False, "error": "invalid credentials"}
            uid, ph, salt, role, tier = row
            ok, new_hash = self._verify_password(ph, password, salt=salt, migrate=True)
            if not ok:
                return {"ok": False, "error": "invalid credentials"}
            # Migrate legacy SHA-256 hash to scrypt if needed
            if new_hash is not None:
                conn.execute("UPDATE members SET password_hash=? WHERE id=?", (new_hash, uid))
                conn.commit()
            token = secrets.token_hex(32)
            now = time.time()
            conn.execute(
                "INSERT INTO sessions (user_id, token, created_at, expires_at, ip_address) VALUES (?,?,?,?,?)",
                (uid, token, now, now + 86400, ""),
            )
            conn.execute("UPDATE members SET last_login=? WHERE id=?", (now, uid))
            conn.commit()
            return {"ok": True, "user_id": uid, "username": username, "token": token, "role": role, "tier": tier}

    def validate_session(self, token: str) -> dict | None:
        """Resolve a session token to its member info.

        Results are memoised for SESSION_CACHE_TTL seconds. The dashboard polls
        at 1 Hz per connected client; without this cache every poll would hit
        SQLite for an identical answer.
        """
        if not token:
            return None
        now = time.time()
        cached = self._sess_cache.get(token)
        if cached is not None and (now - cached[0]) < self.SESSION_CACHE_TTL:
            return cached[1]
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT s.user_id, m.username, m.role, m.tier FROM sessions s JOIN members m ON s.user_id=m.id WHERE s.token=? AND s.expires_at>? LIMIT 1",
                (token, now),
            ).fetchone()
            info = None if not row else {
                "user_id": row[0], "username": row[1], "role": row[2], "tier": row[3],
            }
            self._sess_cache[token] = (now, info)
            if len(self._sess_cache) > 512:
                cutoff = now - self.SESSION_CACHE_TTL
                self._sess_cache = {
                    k: v for k, v in self._sess_cache.items() if v[0] >= cutoff
                }
        return info

    def invalidate_session(self, token: str) -> None:
        """Drop a memoised session (called on logout / member deletion)."""
        self._sess_cache.pop(token, None)

    def logout(self, token: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            cur = conn.execute("DELETE FROM sessions WHERE token=?", (token,))
            conn.commit()
        self._sess_cache.pop(token, None)
        return cur.rowcount > 0

    def update_leaderboard(self, user_id: int, pnl: float, trade_count: int, win_rate: float) -> None:
        with self._lock:
            conn = self._get_conn()
            now = time.time()
            conn.execute(
                """INSERT INTO leaderboard (user_id, total_pnl, trade_count, win_rate, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(user_id) DO UPDATE SET
                     total_pnl=excluded.total_pnl, trade_count=excluded.trade_count,
                     win_rate=excluded.win_rate, updated_at=excluded.updated_at""",
                (user_id, pnl, trade_count, win_rate, now),
            )
            conn.commit()

    def get_leaderboard(self, limit: int = 20) -> list[dict]:
        with self._lock:
            conn = self._get_conn()
            rows = conn.execute(
                """SELECT m.username, m.tier, l.total_pnl, l.trade_count, l.win_rate
                   FROM leaderboard l JOIN members m ON l.user_id=m.id
                   ORDER BY l.total_pnl DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [{"rank": i+1, "username": r[0], "tier": r[1], "total_pnl": round(r[2], 2),
                 "trade_count": r[3], "win_rate": round(r[4], 3)} for i, r in enumerate(rows)]

    def get_my_stats(self, token: str) -> dict | None:
        info = self.validate_session(token)
        if not info:
            return None
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT total_pnl, trade_count, win_rate FROM leaderboard WHERE user_id=?",
                (info["user_id"],),
            ).fetchone()
        if row:
            return {"username": info["username"], "tier": info["tier"],
                    "total_pnl": round(row[0], 2), "trade_count": row[1], "win_rate": round(row[2], 3)}
        return None

    def get_all_members(self) -> list[dict]:
        with self._lock:
            conn = self._get_conn()
            rows = conn.execute(
                "SELECT id, username, email, role, tier, created_at, last_login FROM members ORDER BY created_at"
            ).fetchall()
        return [{"id": r[0], "username": r[1], "email": r[2], "role": r[3], "tier": r[4],
                 "created_at": r[5], "last_login": r[6]} for r in rows]

    def update_member(self, target_id: int, role: str | None = None, tier: str | None = None) -> dict:
        with self._lock:
            conn = self._get_conn()
            sets = []
            vals = []
            if role:
                sets.append("role=?")
                vals.append(role)
            if tier:
                sets.append("tier=?")
                vals.append(tier)
            if not sets:
                return {"ok": False, "error": "no fields to update"}
            vals.append(target_id)
            conn.execute(f"UPDATE members SET {', '.join(sets)} WHERE id=?", vals)
            conn.commit()
            return {"ok": True}

    def delete_member(self, target_id: int) -> dict:
        with self._lock:
            conn = self._get_conn()
            # Only refuse when the target IS an admin and would leave the site
            # with no administrator at all.
            target_role = conn.execute(
                "SELECT role FROM members WHERE id=?", (target_id,)
            ).fetchone()
            if target_role and target_role[0] == "admin":
                admins = conn.execute(
                    "SELECT COUNT(*) FROM members WHERE role='admin'"
                ).fetchone()[0]
                if admins <= 1:
                    return {"ok": False, "error": "must keep at least one admin"}
            tokens = [r[0] for r in conn.execute(
                "SELECT token FROM sessions WHERE user_id=?", (target_id,)
            ).fetchall()]
            conn.execute("DELETE FROM sessions WHERE user_id=?", (target_id,))
            conn.execute("DELETE FROM user_settings WHERE user_id=?", (target_id,))
            conn.execute("DELETE FROM members WHERE id=?", (target_id,))
            conn.commit()
        for tk in tokens:
            self._sess_cache.pop(tk, None)
        return {"ok": True}

    # ── user_settings CRUD ──────────────────────────────────────────────────

    def get_user_settings(self, user_id: int) -> dict | None:
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT user_id, api_key, api_secret, api_passphrase,"
                " risk_preference, trade_mode, run_start_time, run_end_time,"
                " max_position_usd, min_trade_usd, max_trade_usd, trade_usd,"
                " allowed_tickers, take_profit_pct, stop_loss_pct,"
                " updated_at FROM user_settings WHERE user_id=?",
                (user_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "user_id": row[0], "api_key": row[1], "api_secret": row[2],
            "api_passphrase": row[3], "risk_preference": row[4],
            "trade_mode": row[5], "run_start_time": row[6],
            "run_end_time": row[7], "max_position_usd": row[8],
            "min_trade_usd": row[9], "max_trade_usd": row[10],
            "trade_usd": row[11],
            "allowed_tickers": row[12], "take_profit_pct": row[13],
            "stop_loss_pct": row[14], "updated_at": row[15],
        }

    def get_users_with_api_keys(self) -> list[dict]:
        """Members that completed their OKX credential setup.

        Used by the account refresher to decide whose account snapshot to keep
        warm. Returns only the fields needed to build an executor.
        """
        with self._lock:
            conn = self._get_conn()
            rows = conn.execute(
                """SELECT u.user_id, u.api_key, u.api_secret, u.api_passphrase,
                          u.risk_preference, u.trade_mode,
                          u.min_trade_usd, u.max_trade_usd, u.max_position_usd,
                          u.trade_usd, u.take_profit_pct, u.stop_loss_pct
                   FROM user_settings u JOIN members m ON u.user_id = m.id
                   WHERE TRIM(COALESCE(u.api_key,'')) <> ''
                     AND TRIM(COALESCE(u.api_secret,'')) <> ''
                     AND TRIM(COALESCE(u.api_passphrase,'')) <> ''"""
            ).fetchall()
        keys = ("user_id", "api_key", "api_secret", "api_passphrase",
                "risk_preference", "trade_mode", "min_trade_usd", "max_trade_usd",
                "max_position_usd", "trade_usd", "take_profit_pct", "stop_loss_pct")
        return [dict(zip(keys, r)) for r in rows]

    def upsert_user_settings(self, user_id: int, settings: dict) -> dict:
        with self._lock:
            conn = self._get_conn()
            existing = conn.execute(
                "SELECT id FROM user_settings WHERE user_id=?", (user_id,)
            ).fetchone()
            now = time.time()
            if existing:
                sets = ", ".join(f"{c}=?" for c in self.USER_SETTINGS_COLUMNS)
                vals = tuple(settings.get(c, self.USER_SETTINGS_DEFAULTS.get(c))
                             for c in self.USER_SETTINGS_COLUMNS)
                conn.execute(
                    f"UPDATE user_settings SET {sets}, updated_at=? WHERE user_id=?",
                    (*vals, now, user_id),
                )
            else:
                self._insert_user_settings(conn, user_id, settings)
            conn.commit()
            return {"ok": True}

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
