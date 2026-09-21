# OKX Trade Kit — 多会员实时交易仪表盘

基于 Python + SQLite + OKX API 的多会员实时交易桌面/服务端，每个会员可拥有独立交易规则（允许币种、仓位、滑点等）。

## 架构

- `rh_server_live.py`：主 HTTP 服务（端口 8765），内嵌 HTML/CSS/JS 仪表盘，SSE 实时推送
- `rh_desk_runner.py`：Desk 信号执行器
- `rh_okx_live_engine.py` / `rh_okx_perp_engine.py`：OKX 现货/合约引擎
- `rh_okx_executor.py`：OKX API 执行器
- `rh_jupiter_executor.py`：Jupiter Solana 执行器
- `db_trades.py`：SQLite 数据层（`user_settings` 表存储每个会员独立交易规则）
- `user_account.py`：多会员账户上下文（每个会员一套自己的 OKX API Key + 账户快照缓存）
- `run_live.py`：入口脚本

## 多会员数据隔离

仪表盘展示哪一套账户数据，由「当前登录会员是否已绑定自己的 OKX API Key」决定：

| 状态 | 仪表盘数据来源 |
| --- | --- |
| 未登录 / 未绑定 API Key | 系统默认数据（共享交易台，完全不变） |
| 已登录 + 已保存 API Key（快照未就绪） | 会员专属视图，标记 `account_pending`，**不泄露**系统账户数据 |
| 已登录 + 已保存 API Key（快照就绪） | 该会员自己的余额 / 持仓 / 成交 / 盈亏 |

关键实现点：

- **数据分租**：`closed_trades` / `open_trades` 带 `user_id` 列，`0` 表示系统共享数据；`open_trades` 唯一键为 `(ticker, user_id)`，不同会员可持有同一币种。
- **请求路径零阻塞**：`UserAccountManager` 后台线程按 `USER_ACCOUNT_REFRESH_SEC`（默认 30s）轮询有 API Key 的会员账户，`/api/desk-state` 只读内存缓存。冷缓存时返回 `account_pending` 并唤醒后台刷新，绝不等待 OKX。
- **盈亏基准**：会员的「今日盈亏」以其**当日首次快照权益**为基准（持久化在 `settings` 表的 `acct_baseline_<uid>`），不使用系统交易台的起始金额。
- **下单金额区间**：`user_settings.min_trade_usd` / `max_trade_usd` 是**开仓**单笔金额上下限；平仓（一键平仓、止盈止损、超时平仓）不受限制，避免仓位被限额卡死。
- **一键平仓**：`POST /api/position/close`，`{kind: "perp"|"spot", inst_id, ticker, side, size_usd}`。始终使用会员**自己的**凭据执行，系统共享账户无法从浏览器被操作；未绑定 API Key 时返回 403 `no_api_key`。

## 性能要点

- `/api/desk-state` 支持 gzip，并对「同一状态代 + 同一租户」的编码结果做跨客户端复用（N 个客户端 1Hz 轮询 → 每 tick 只编码一次）。
- `TradeDB.validate_session` 带 5s 内存缓存，1Hz 轮询不再每次打 SQLite。
- SQLite 开启 WAL + `cache_size` / `temp_store=MEMORY` / `mmap_size`，并针对 `(user_id, id DESC)`、`(user_id, win)` 建索引。
- `rh_okx_executor` 使用线程局部 `requests.Session` 复用 TCP/TLS 连接。
- `DeskRunner` 的散点图 SVD 投影按间隔缓存（`_scatter_cache_refresh`），不再每 tick 重算。

## 多语言（i18n）

前端是单页内嵌模板，翻译全部走 `I18N` 字典（`en` / `zh` 双侧必须同时定义）：

- 静态元素标 `data-i18n="key"`；输入框占位符用 `data-i18n-ph="key"`。
- 动态字符串（表格单元格、状态文案）用 `t("key")`，不要再写 `lang==='zh' ? '中文' : 'English'` 裸三元。
- `applyLang()` 对**缺键会 `console.warn` 并回落英文**，不再静默保留写死的英文原文。
- 元素内若有 `<br>` 等标记时按 `innerHTML` 整体替换，避免只替换第一个文本节点导致第二段永远不翻译。
- `setLang()` 会调 `rerenderI18n()` 立即重渲染当前页，不必等下一个 1Hz 轮询。
- 语言选择持久化在 `localStorage.rh_lang`。

新增文案时先加字典键，再加 `data-i18n`，最后跑一次全站扫描确认 `data-i18n` 引用键都有定义。

## 本地运行

```bash
pip install -r requirements.txt
python run_live.py
# 浏览器访问 http://127.0.0.1:8765
```

## 线上部署（Ubuntu 24.04）

```bash
# 1. 创建运行目录
sudo mkdir -p /opt/okx-tradekit
# 2. 上传代码（从本地）
scp -i ~/.ssh/geodetect_deploy -r ./* root@47.82.1.129:/opt/okx-tradekit/
# 3. 创建虚拟环境并安装依赖
python3 -m venv /opt/okx-tradekit/venv
/opt/okx-tradekit/venv/bin/pip install -r /opt/okx-tradekit/requirements.txt
# 4. 配置 .env（OKX API 密钥、域名等）
# 5. 创建 systemd service 并启动
sudo systemctl enable --now okx-tradekit
# 6. 配置 nginx 反向代理 okx.geoaiglobals.com（不影响 www / shop）
```

> 注意：部署在 `/opt/okx-tradekit`，不影响服务器上已有的 `www.geoaiglobals.com` 和 `shop.geoaiglobals.com`。

## 相关环境变量

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `RH_EXECUTOR` | `jupiter` | `okx` = 走 OKX 现货/合约；`jupiter` = Solana。设为 `okx` 且凭据完整时自动切数据源到 OKX 行情 |
| `OKX_DEMO` | — | `1` = 模拟盘（请求带 `x-simulated-trading: 1`）。**模拟盘 API Key 必须开启**，否则实盘环境报 `50101` |
| `RH_PROXY` | 自动探测 `127.0.0.1:7897` | 出站代理。启动时会**强制覆盖** `HTTP(S)_PROXY`——宿主机常注入只服务于内部、对外一律 502 的代理 |
| `USER_ACCOUNT_REFRESH_SEC` | `30` | 会员账户快照刷新间隔（秒） |
| `OKX_ACCOUNT_REFRESH_SEC` | `30` | 共享交易台账户快照刷新间隔（秒） |
| `OKX_API_KEY` / `OKX_API_SECRET` / `OKX_PASSPHRASE` | — | 共享交易台凭据（会员用自己的 `user_settings`） |
| `OKX_AI_BUILDER_CODE` | — | 所有下单请求携带的 Builder Code |

## 下单权限（重要）

交易台下单受 `trade_mode` 控制（会员设置里的「交易模式」，默认 **仅信号**）：

| 模式 | 行为 |
| --- | --- |
| `signal_only`（默认） | 只生成待处理交易包，**不向交易所发单**；需在页面上人工确认 |
| `auto` | 自动提交待处理交易包 + 自动开仓 |

> 自动平仓（TP/SL/超时）不受 `trade_mode` 限制，始终生效——它只会**减少**风险敞口，且仓位必须能退出。
> 同一仓位的平仓有 30 秒冷却，避免用 400ms 的 tick 反复重复提交同一笔平仓单。


