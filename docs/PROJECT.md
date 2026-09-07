# RH Trencher — AI Agent Trading System

## 项目简介

RH Trencher 是一个由 AI Agent 驱动的自动化加密交易系统。项目名称源自"挖沟者"——像工兵一样在海量市场信号中挖掘出高概率的入场机会。系统以 Desk 状态机为核心引擎，结合 Kelly 仓位计算、Narrative 叙事聚类、Expectancy 期望值风控三道防线，将实时市场数据流转化为可执行的 OKX / Jupiter 交易信号。

GitHub: https://github.com/linkes998/okx-Tradekit  
Dashboard: http://127.0.0.1:8780/ （本地运行）

## 产品定位

RH Trencher 是 OKX AI Builder Program 的交易执行端 Builder。系统对接 OKX V5 REST API 进行行情获取（1390+ spot 交易对）和订单执行，每笔订单自动携带 AI Builder Code（OKX 要求的 `tag` 字段），确保所有通过 RH Trencher 产生的 OKX 交易自动计入返佣归因。最终用户只需配置自己的 OKX API Key，即可让 AI Agent 全自动运行。

## 技术架构

```
┌────────────────────────────────────────────────────────┐
│                  数据流层                               │
│   DexScreener trending + OKX /api/v5/market/candles    │
│   → 清洗 + 聚类 → Desk Signal Stream                   │
├────────────────────────────────────────────────────────┤
│                  引擎层                                 │
│   Desk FSM (SCANNING → ENTERED → HALTED)              │
│   Kelly Criterion 动态仓位                              │
│   Narrative Cluster 叙事主题                            │
│   Expectancy 期望值风控                                 │
├────────────────────────────────────────────────────────┤
│                  执行层                                 │
│   OKXExecutor ←→ OKX V5 /api/v5/trade/order           │
│   JupiterExecutor ←→ Solana Metis Swap V1            │
│   双执行器可切换, LiveTrader 统一桥接                  │
├────────────────────────────────────────────────────────┤
│                  前端                                   │
│   rh_server_live.py — Python stdlib HTTP server        │
│   SSE + 1s polling Dashboard                          │
│   Phantom 钱包签名 + 订单状态追踪                      │
└────────────────────────────────────────────────────────┘
```

## 核心模块（23 Python 模块 · ~5300 行）

| 模块 | 行数 | 职责 |
|------|------|------|
| `rh_trencher.py` | 1259 | Desk 状态机 + FSM 转换 + token lifecycle |
| `rh_server_live.py` | 1212 | Dashboard + SSE + REST API + Tooltip |
| `rh_okx_executor.py` | 497 | **OKX V5 REST 签名 + 下单 + tag 注入** |
| `rh_live_trader.py` | 535 | Desk signal_callback → quote → build_bundle 桥接 |
| `rh_desk_runner.py` | 388 | tick 循环 + 自动拉新 + replay loop |
| `rh_jupiter_executor.py` | 435 | Solana Jupiter Metis Swap（双执行器之一） |
| `fetch_dex_top.py` | 394 | DexScreener trending + top-volume fetch |
| `pool_lake.py` | 718 | OHLCV 存储 + 回测数据池 |

## OKX API 使用场景

RH Trencher 使用 OKX V5 REST API 的以下端点：

**行情（public, 无需签名）**
- `GET /api/v5/market/ticker` — 实时价格 + 24h 成交量
- `GET /api/v5/market/candles` — 1m/5m K 线用于 Desk 信号判断
- `GET /api/v5/market/books` — orderbook 深度估算滑点
- `GET /api/v5/public/instruments` — 1390+ spot 交易对白名单

**交易（signed, 需 OKX API Key）**
- `POST /api/v5/trade/order` — **市价/限价下单, `tag` 字段自动写入 AI Builder Code**
- `GET /api/v5/trade/order/{orderId}` — 订单状态查询
- `POST /api/v5/trade/cancel-order` — 撤单
- `GET /api/v5/account/balance` — 账户余额（用于 Kelly 仓位计算）

### AI Builder Code 注入（返佣关键）

```python
# rh_okx_executor.py 核心逻辑
payload = {
    "instId": order.inst_id,        # "BTC-USDT"
    "tdMode": "spot",               # OKX 要求的交易模式
    "side": order.side,             # "buy" / "sell"
    "ordType": "market",            # "market" / "limit"
    "sz": order.sz,                 # 数量
    "tag": order.tag,               # ← AI Builder Code 在这里,OKX 自动归因
}
```

每笔订单的 `tag` 字段 = Builder Code（从 `.env` 读取 `OKX_AI_BUILDER_CODE`），确保 OKX 控制台可追踪每笔交易并计算返佣。

## 运行方式

```bash
# 1. 配置环境变量
export OKX_API_KEY="your_okx_api_key"
export OKX_API_SECRET="your_okx_api_secret"
export OKX_PASSPHRASE="your_okx_passphrase"
export OKX_AI_BUILDER_CODE="YOUR_BUILDER_CODE"  # OKX Builder 控制台获取
export OKX_DEMO="1"                              # 先用模拟盘测试

# 2. 启动 Dashboard
python rh_server_live.py --port 8780

# 3. 浏览器打开 http://127.0.0.1:8780
#    DeskRunner 自动拉新 OKX/DEX 交易对 → 发出 BUY/SELL 信号
#    每笔信号 → LiveTrader 调用 OKXExecutor.get_quote() → build_order()
#    用户点 Dashboard 上的 ▶ 提交 → 订单自动携带 tag 上链 OKX
```

## 项目价值

| 维度 | 说明 |
|------|------|
| **AI Builder 贡献** | 每笔 OKX 订单自动带 Builder Code 返佣归因,零额外成本 |
| **智能信号** | Desk FSM + Kelly + Narrative 三层风控,避免裸奔策略 |
| **双执行器** | OKX CEX + Jupiter DEX 可切换,一套策略跑两个战场 |
| **实时 Dashboard** | 信号流、资金曲线、Swap 列表、Desk 状态一站式 |
| **可回测** | pool_lake.py 支持历史 OHLCV 回测 Desk 表现 |

## 项目状态

- [x] Desk 引擎核心 (Kelly / Narrative / Expectancy)
- [x] DeskRunner 无限 replay + 自动拉新币
- [x] OKX V5 REST Executor (签名 + tag 注入 + ping 验证)
- [x] Jupiter Solana Executor
- [x] LiveTrader 桥接层 (signal_callback → quote → build_bundle)
- [x] Dashboard + Phantom 钱包签名 + Tooltip
- [x] Git 仓库已推送到 github.com/linkes998/okx-Tradekit
- [ ] OKX API Key 申请 (正在进行)
- [ ] OKX AI Builder Code 申请 (正在进行)
- [ ] OKX 模拟盘小额订单测试
- [ ] OKX OI / Funding Rate 增强 Desk 信号
- [ ] OKX OAuth 多用户托管模式
