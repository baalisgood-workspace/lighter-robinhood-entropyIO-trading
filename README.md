# lighter-robinhood-entropyIO-trading

三个交易所之间的 **maker → taker 价差套利**机器人：

| 名称 (`venues.*`) | 交易所 | 说明 |
|---|---|---|
| `lighter` | Lighter 主网永续 | 手续费 0 |
| `lighter_rh` | Lighter Robinhood Chain | 手续费 0，chain id 466324 |
| `entropy` | Entropy（Hyperliquid HIP-3，dex = `io`） | `io:OAI` / `io:ANTH` / `io:SNDK`，growth mode 费率 maker 0.003% / taker 0.009% |

三个交易所都可以开/关，但 **交易时永远只有两个开启，两两对冲**。`monitor` 模式可以同时看三组价差。

## 工作原理

1. **行情**：每个交易所一条公共 WebSocket（Lighter 用 `?readonly=true`，任何地区都能连），维护本地订单簿。
2. **基差（basis）**：同一标的在两边存在结构性价差（例如 OPENAI 在 Lighter ≈ 1590，Entropy ≈ 1524，约 4%）。
   机器人用 1 小时时间加权 EMA 跟踪基差，只交易 **相对基差的偏离（dev）**，不是原始价差。
   EMA 需预热 `warmup_seconds`（默认 1 小时）才会交易；断线会进入恢复期，长时间断线会重置。状态保存在 `data/basis_state.json`，重启不必重新预热（10 分钟内）。
3. **开仓信号**：`dev ≥ T1 + (k-1)·step + 往返手续费 + 不利资金费` → 开第 k 层网格。
4. **执行**：在 maker 交易所挂 post-only 单（跟随最优价，不在最优就撤单重挂），每成交一部分就立即在 taker 交易所用 IOC 对冲；对冲失败会逐步加大滑点重试，仍失败则在 maker 端 reduce-only 紧急平掉，再失败就 **HALT**（停止并报警）。
5. **平仓**：dev 回落到平仓线（第 1 层 = T1×`first_close_ratio`，第 k 层 = 第 k-1 层的开仓线），同样 maker→taker（或 `close_mode: taker_taker`）。
6. **风控**：报价新鲜度、深度/滑点带、价格稳定性、资金费过滤、余额下限、单标的/单交易所名义上限、仓位对账（两次不一致即 HALT）、连续失败暂停、单实例锁、退出时撤单。

## 交易所开关与角色

`config/config.yaml`：

```yaml
venues:
  lighter:    {enabled: true}
  lighter_rh: {enabled: true}
  entropy:    {enabled: false}
roles:                      # 每一对里谁挂 maker 单
  lighter+lighter_rh: lighter_rh
  lighter+entropy:    entropy
  lighter_rh+entropy: entropy
```

也可以不改文件，直接在命令行切换：

```bash
python -m arb live --venues lighter,entropy        # Lighter 主网 ↔ Entropy
python -m arb live --venues lighter_rh,entropy     # Lighter RH ↔ Entropy
python -m arb live --venues lighter,lighter_rh     # Lighter 主网 ↔ Lighter RH
```

- 开启超过 2 个 → 配置报错；`live` 必须正好 2 个。
- 每个标的在 `symbols` 里写出各交易所的市场名；某一对只交易两边都有的标的（Entropy 目前只有 OPENAI / ANTHROPIC / SNDK 三个重叠）。
- **切换交易对前先平仓**：如果 `data/positions.json` 里另一对还有仓位，`live` 会拒绝启动。先用原来那两个交易所 + `--close-only` 跑到平仓。

## 安装

需要 Python 3.10+（Linux / macOS；Lighter 签名库不支持 Windows 原生，请用 WSL）。

```bash
./start.sh setup          # 建 .venv、装依赖、复制 .env
# 编辑 .env（只填要交易的两个交易所）和 config/config.yaml
./start.sh check          # 离线检查配置、显示交易对和标的映射
./start.sh monitor        # 只看行情和价差，不下单（建议至少跑一天）
./start.sh report         # 根据 data/spread_samples/*.csv 统计 dev 分布
./start.sh positions      # 交易所余额/持仓 + 本地状态
./start.sh live           # 实盘（会显示配置并要求输入 yes）
./start.sh live --close-only   # 只平不开
```

## 凭证

**Lighter 主网 / Lighter RH**：`*_ACCOUNT_INDEX`、`*_API_KEY_INDEX`、`*_API_KEY_PRIVATE_KEY`。
RH 的 API key index 0–3 和 157 是保留的，请用 4–156 或 158–254；RH 普通账户请求限额较低，默认 `requests_per_minute: 60`。

**Entropy**：Hyperliquid 账户地址 `ENTROPY_ACCOUNT_ADDRESS` + 已授权的 API（agent）钱包私钥 `ENTROPY_API_PRIVATE_KEY`（agent 钱包不能提现）。
注意：HIP-3 dex 的保证金和 Hyperliquid 主 perp 账户是分开的，需要先把 USDC 转入 `io` dex（Entropy 网页或 Hyperliquid 的 dex 间划转）。OAI 等只支持逐仓，启动时会按 `venues.entropy.leverage` 设置逐仓杠杆（OAI 最高 6x）。
Entropy 条款限制部分地区（含美国、加拿大、欧盟等），请自行确认合规。

**建议**：每个交易所用专用子账户，不要手动在同一账户交易；否则对账会不一致并 HALT。

## 主要参数（`defaults` 下，可按标的覆盖）

| 参数 | 默认 | 含义 |
|---|---|---|
| `strategy.initial_threshold_pct` | 0.30 | T1，第一层开仓 dev（%，另加手续费/资金费） |
| `strategy.grid_step_pct` | 0.15 | 每层增加 |
| `strategy.max_grids` | 3 | 最多层数 |
| `strategy.grid_value_usdc` / `order_value_usdc` | 200 / 100 | 每层名义 / 每笔 maker 单名义 |
| `strategy.session` | 24x7 | 美股标的用 `us_equity` + `offhours_threshold_pct`（盘外提高阈值） |
| `basis.ema_seconds` / `warmup_seconds` | 3600 | 基差 EMA 时间常数 / 预热时间 |
| `execution.maker_timeout_s` | 20 | maker 单最长挂单时间 |
| `execution.hedge_slippage_pct` | 0.05 | 对冲 IOC 限价滑点（每次重试 +0.05） |
| `risk.max_symbol_value_usdc` | 600 | 单标的最大名义 |
| `risk.max_venue_notional_usdc` | 2000 | 单交易所最大名义 |
| `risk.min_available_usdc` | 100 | 可用余额下限 |

Entropy 费率：基础 maker 0.030% / taker 0.090%，growth mode 标的为 1/10。若某标的不在 growth mode，请把 `venues.entropy` 费率改为 0.03 / 0.09（往返成本会显著上升，T1 也要相应调高）。

## 目录

```
arb/
  cli.py            命令行
  config.py         配置解析与校验（两两规则、角色）
  venues/lighter.py Lighter 主网 + RH（同一实现，不同 URL/chain id/凭证前缀）
  venues/entropy.py Entropy / Hyperliquid HIP-3
  engine/basis.py   基差 EMA
  engine/signals.py 信号、网格线、VWAP
  engine/executor.py maker→taker 执行、增量对冲、紧急平仓
  engine/state.py   网格仓位与残差（data/positions.json）
  engine/engine.py  主循环、决策、对账、采样
  ui.py / report.py 终端面板 / 价差统计
tests/              单元测试（pytest）
```

## 风险与未验证项（务必先小资金）

- 公共行情、价差计算、采样已对真实行情验证；**下单路径只在模拟交易所上测试过**，没有用真实凭证下过单。第一次实盘请把 `grid_value_usdc` / `order_value_usdc` 调到最小（≥ $10–15），盯盘观察。
- Lighter 的 taker 腿使用 `ORDER_TYPE_MARKET + IOC`（价格为最差成交价），与 SDK `create_market_order` 一致。
- Entropy 的 `orderUpdates` 推送在 HIP-3 上若有延迟，会自动退回 REST `orderStatus` 轮询。
- 两条 Lighter 链 / Hyperliquid 之间资金不能自动划转；某一边保证金不足时机器人只会停止开仓，需要手动调仓。
- 基差会漂移（尤其是 OPENAI/ANTHROPIC 这种盘前标的），`max_drift_pct` 会在基差快速变化时暂停交易。

```bash
pip install -r requirements-dev.txt && python -m pytest -q
```
