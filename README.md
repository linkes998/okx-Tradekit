# OKX Trade Kit — 多会员实时交易仪表盘

基于 Python + SQLite + OKX API 的多会员实时交易桌面/服务端，每个会员可拥有独立交易规则（允许币种、仓位、滑点等）。

## 架构

- `rh_server_live.py`：主 HTTP 服务（端口 8765），内嵌 HTML/CSS/JS 仪表盘，SSE 实时推送
- `rh_desk_runner.py`：Desk 信号执行器
- `rh_okx_live_engine.py` / `rh_okx_perp_engine.py`：OKX 现货/合约引擎
- `rh_okx_executor.py`：OKX API 执行器
- `rh_jupiter_executor.py`：Jupiter Solana 执行器
- `db_trades.py`：SQLite 数据层（`user_settings` 表存储每个会员独立交易规则）
- `run_live.py`：入口脚本

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
