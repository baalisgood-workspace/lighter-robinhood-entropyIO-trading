# lighter-robinhood-entropyIO-trading

四个交易所之间的 **maker → taker 价差套利**机器人：

| 名称 (`venues.*`) | 交易所 | 说明 |
|---|---|---|
| `lighter` | Lighter 主网永续 | 手续费 0 |
| `lighter_rh` | Lighter Robinhood Chain | 手续费 0，chain id 466324 |
| `entropy` | Entropy（Hyperliquid HIP-3，dex = `io`） | `io:OAI` / `io:ANTH` / `io:SNDK`，growth mode 费率 maker 0.003% / taker 0.009% |
| `arcus` | Arcus 永续 | 市场名 `SNDK-USD` 等；Base 档 maker 0 / taker 0.0225% |

每个进程只交易 **一对**（两个交易所对冲）。想同时跑多对，就 **多开几个进程，每个用 `--venues` 选两个交易所**（见下文“多开”）。`monitor` 会同时显示全部 6 组价差。

## 工作原理

1. **行情**：每个交易所一条公共 WebSocket（Lighter 用 `?readonly=true`，任何地区都能连），维护本地订单簿。
2. **基差（basis）**：同一标的在两边存在结构性价差（例如 OPENAI 在 Lighter ≈ 1590，Entropy ≈ 1524，约 4%）。
   机器人用 1 小时时间加权 EMA 跟踪基差，只交易 **相对基差的偏离（dev）**，不是原始价差。
   EMA 需预热 `warmup_seconds`（默认 1 小时）才会交易；断线会进入恢复期，长时间断线会重置。状态保存在 `data/basis_state.json`，重启不必重新预热（10 分钟内）。
3. **开仓信号**：`dev ≥ T1 + (k-1)·step + 往返手续费 + 不利资金费` → 开第 k 层网格。
4. **执行**：在 maker 交易所挂 post-only 单（跟随最优价，不在最优就撤单重挂），每成交一部分就立即在 taker 交易所用 IOC 对冲；对冲失败会逐步加大滑点重试（只有“确认零成交”的 IOC 才消耗重试次数），仍失败则 **HALT + 人工介入锁**：撤掉所有 maker 单、保留两边仓位、写入 `data/<交易对>/MANUAL_INTERVENTION.json`，重启后也不会自动恢复（可选 `emergency_unwind: true` 先在 maker 端 IOC 平掉）。
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
  lighter+arcus:      arcus     # Arcus maker 0 费，Lighter taker 0 费
  lighter_rh+arcus:   arcus
  entropy+arcus:      arcus     # 在 Entropy 吃单（0.009%）比在 Arcus 吃单（0.0225%）便宜
```

也可以不改文件，直接在命令行切换：

```bash
python -m arb live --venues lighter,entropy        # Lighter 主网 ↔ Entropy
python -m arb live --venues lighter_rh,entropy     # Lighter RH ↔ Entropy
python -m arb live --venues lighter,lighter_rh     # Lighter 主网 ↔ Lighter RH
python -m arb live --venues lighter,arcus          # Lighter 主网 ↔ Arcus
python -m arb live --venues lighter_rh,arcus       # Lighter RH ↔ Arcus
python -m arb live --venues entropy,arcus          # Entropy ↔ Arcus（目前只有 SNDK 重叠）
```

各对重叠标的（默认配置）：

| 交易对 | 标的 |
|---|---|
| lighter + lighter_rh | OPENAI、ANTHROPIC、SNDK、美股（MU/AMD/NVDA/INTC/SPCX/TSLA/AAPL/AMZN/GOOGL/META/MSFT/PLTR/COIN/SPY/QQQ）、BTC/ETH/SOL/HYPE |
| lighter / lighter_rh + entropy | OPENAI、ANTHROPIC、SNDK |
| lighter + arcus | SNDK、上面的美股、NBIS、DRAM、BTC/ETH/SOL/HYPE |
| lighter_rh + arcus | SNDK、上面的美股、BTC/ETH/SOL/HYPE |
| entropy + arcus | SNDK |

## 多开（并行跑多对）

每对一个进程（一个终端 / tmux 窗口 / systemd 服务）：

```bash
./start.sh live --venues lighter_rh,entropy --yes
./start.sh live --venues lighter,arcus --yes
```

- 每对的状态独立保存在 `data/<交易对>/`（`positions.json`、`basis_state.json`、`spread_samples/`、`arb.lock`），同一对不能同时开两个进程。
- **共用账户保护**：如果两个进程用的是同一个交易所账户（例如都用同一个 Lighter 主网账户），同一个标的只能被一个进程交易——先启动的进程会锁住 `交易所+账户+标的`（`data/claims/*.lock`），后启动的进程会跳过这个标的（面板显示 `skipped`）。某进程停了但还留有仓位时，该标的仍归它，直到平仓。这样可以避免两边仓位互相抵消、reduce-only 平仓被拒、对账误报、撤单撤到别的进程的单。
- 想让两个进程交易同一个标的，就给其中一个用不同的子账户（Lighter 子账户 / Hyperliquid 子账户 / Arcus `ARCUS_ACCOUNT_INDEX` 0–9）。
- 余额是共用的：每个进程各自按 `risk` 上限控制自己的仓位，总敞口 = 各进程之和，请据此设置上限。
- `./start.sh positions` 会列出所有交易对的本地状态；`./start.sh report` 汇总所有目录的价差采样。

- 开启超过 2 个 → 配置报错；`live` 必须正好 2 个。
- 每个标的在 `symbols` 里写出各交易所的市场名；某一对只交易两边都有的标的（Entropy 与 Lighter 重叠 OPENAI / ANTHROPIC / SNDK，与 Arcus 只重叠 SNDK）。
- 停掉一对之前建议先 `--close-only` 跑到平仓；留着仓位也可以，下次启动同一对会继续管理（并对账）。

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

**Arcus**：`ARCUS_ACCOUNT_ADDRESS`（0x 地址）、`ARCUS_ACCOUNT_INDEX`（0–9）、`ARCUS_API_KEY`（Ed25519 公钥 hex）、`ARCUS_API_SIGNING_KEY`（对应 32 字节种子 hex）。启动时会校验公钥和种子是否匹配。杠杆/保证金模式请在 Arcus 网页设置。美股标的盘外有价格上下限（trading bounds），超出 mark 10% 的保护价会被拒绝。费率见 https://api.arcus.xyz/v1/feetiers，升级档位后请改 `venues.arcus.taker_fee_pct`。

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
| `strategy.min_close_profit_pct` / `close_floor_timeout_s` | 0.01 / 600 | 平仓利润地板（见下） |
| `execution.maker_price_mode` | join | `join` / `improve` / `threshold`（见下） |
| `execution.hedge_min_notional_usdc` | 10 | 部分成交累积到此名义才对冲 |
| `execution.emergency_unwind` | false | 对冲失败是否自动在 maker 端平掉 |
| `risk.warmup_fresh_s` / `quick_resume_s` | 60 / 10 | 连续新鲜行情预热 / 短断线保留预热 |
| `risk.max_leverage` / `reserve_ratio` | 2 / 0.02 | 单交易所总名义 ≤ 权益×(1−预留)×杠杆 |
| `risk.max_unhedged_notional_usdc` | 500 | 未对冲残差超过即停止开仓 |
| `trading.max_parallel_slices` | 1 | 同时执行的 maker 单（不同标的）数量 |

## 从 arcus-maker-lighter-taker 移植的改进

以下改进来自 `baalisgood/arcus-maker-lighter-taker`（main @7ed5f74），已改写为与交易所无关的实现，四个交易所、所有交易对都生效：

1. **行情新鲜度 / 心跳**（`venues/transport.py`）：所有收到的帧（包括协议 ping/pong 控制帧）都刷新存活时间；每 `liveness_s/2`（默认 2.5s）主动 ping，`liveness_s + quick_resume_s` 内无回应即断开重连；重连 / 序号断档（Lighter `begin_nonce` 不连续）/ Arcus `degraded` 都会作废该标的盘口（epoch+1），并写入健康告警。
2. **连续新鲜预热**：两边行情连续新鲜 `warmup_fresh_s`（60s）后才允许挂单；同一连接内 < `quick_resume_s` 的短暂中断保留预热，重连或断档则重新预热。按标的隔离：一个标的行情不新鲜只影响它自己。
3. **阈值挂单模式** `maker_price_mode: threshold`：maker 单挂在“刚好满足所需 dev（以基差 EMA 为中心）”的价格上并钳到最近的被动 tick，等行情过来成交；公允价变动 > `requote_threshold_pct`（0.006%）或挂单超过 `max_order_age_s`（30s）就改价，每标的每秒最多 `max_requotes_per_s`（2）次，风控撤单不受限；撤单确认终态后才挂新单。建议配合 `trading.max_parallel_slices > 1` 和 maker 端限频宽松的交易所（Arcus / Entropy）使用。
4. **FIFO 平仓利润地板**：每层网格记录开仓 maker/taker 实际成交均价对应的原始价差（FIFO 批次）；在 `close_floor_timeout_s`（600s）内的批次，只有“开仓原始价差 + 平仓原始价差 ≥ `min_close_profit_pct`”才平仓（不受基差漂移影响，只会让平仓更保守）；超时的批次回到原有 EMA 平仓规则。
5. **碎单累积**：部分成交累积到满足交易所最小量和 `hedge_min_notional_usdc`（10U）才对冲；更小的残差计入风险，并在下一笔同方向切片里一起对冲，不再单独在 maker 端市价平掉。
6. **对冲重试预算**：只有确认零成交的 IOC 才计入 `hedge_max_attempts`，部分成交继续对冲（`hedge_max_orders` 硬上限 12）。
7. **严重故障 → 人工介入锁**：任何 HALT 都会撤掉所有 maker 单、保留仓位、持久化第一次故障原因到 `MANUAL_INTERVENTION.json`；该交易对重启会拒绝运行，核对两边仓位后执行 `python -m arb unlock --venues a,b` 解锁。
8. **资金约束**：单交易所总名义（含进行中的切片）≤ 权益 ×(1−2%)× `max_leverage`(2)；未对冲名义 > 500U 停止开仓；对账时暂停新切片，等进行中的切片结束后再对账。
9. **诊断日志**：`data/<交易对>/events.jsonl` 记录每笔开/平（成交均价、原始/调整价差、地板、改价次数、零成交对冲次数、残差、毛利）、HALT、行情健康告警（reconnect / connection_timeout / sequence_gap / degraded）、各标的可交易时长占比；`python -m arb report` 会额外输出成交统计。
10. **下单安全**：HTTP / WebSocket 一律拒绝重定向（被重定向的写请求视为结果未知）；Lighter / Arcus 每账户签名串行，maker 单在拿到写锁后、签名前再检查行情连接。

未移植：SQLite 持久账本、paper 撮合模拟、Lighter/Arcus 端点硬编码白名单、原仓库的单向 Arcus→Lighter 专用逻辑（本项目四交易所通用，结构不同）。

Entropy 费率：基础 maker 0.030% / taker 0.090%，growth mode 标的为 1/10。若某标的不在 growth mode，请把 `venues.entropy` 费率改为 0.03 / 0.09（往返成本会显著上升，T1 也要相应调高）。

## v5.6：卡死的小残仓自动清理 / 残仓不再锁方向 / 没成交的信号写日志

**1. 小残仓自动清理（`risk.dust_flatten`，默认开）**：某个网格因为低于交易所最小下单额而平不掉
（日志 `not closable: ... below the ... minimum order` 或 `close hedge pending: ... (paired off after the reconcile)`），
持续 `dust_grace_s`（默认 120 秒），并且两边交易所的实际持仓**每边都不超过 `dust_max_usdc`（默认 25 USDC）**时，
在对账成功之后用吃单把这个标的两边都平掉：
- 低于最小下单额的一边：先同方向补到最小额（约 1.1 倍最小额），再用 reduce-only 把整笔平掉；
  够最小额的一边（Arcus 的 reduce-only 不受最小金额限制）直接 reduce-only 平掉。
- 完成后本地记录改成交易所的实际持仓（正常情况两边都为 0），记一条 `dust_flatten`，界面事件日志也有提示。
- 没平干净时剩下的记为残仓，至少 10 分钟后才会再试；任何订单结果未知 → 和其他地方一样停机等人工处理。
- 成本：每边多两笔约 10~20U 的吃单，几分钱。超过 `dust_max_usdc` 的不自动处理，只告警一次。
- 关闭：`risk: {dust_flatten: false}`（回到 v5.5：一直等）。

**2. 残仓不再锁方向**：网格平完后只剩一点零头（如 `grids=[] residual maker=-0.004`）时，以前方向仍记着，
反方向的开仓信号会被一直跳过；现在没有网格就不锁方向。

**3. 没成交的开仓信号写日志（`risk.skip_log_s`，默认 300 秒，0 = 关）**：价差已到开仓门槛但没下单时，
按"标的 + 原因"每 5 分钟最多记一次：日志 `signal not taken: ONDO sell_maker dev 0.21% >= need 0.15%: opposite position held (...)`，
`events.jsonl` 记 `signal_skip`。原因包括：`opposite position held`（持有反方向仓位）、`max_grids reached`、
`funding gate`、`size below minimum`、`prices unstable`（价格不稳定）、`blocked`（余额 / 仓位上限 / 杠杆 / 波动禁开等，
细节在 detail 里）、`global pause`、`close_only`（只记事件不写日志）。

仓库根目录的 `why_idle.py` 会汇总这些记录：`python3 why_idle.py --hours 8`。

## v5.5：成交后价格走势统计（markout）

用来回答"挂单成交后价格往哪边走、对冲后到底赚不赚价差"，为以后决定要不要常驻挂单、哪些标的值得做提供数据。不改变任何下单逻辑。

- 每次 maker 成交（开仓、平仓分片都算）后，在 `markout_horizons_s`（默认 `[1, 5, 30, 60]` 秒，范围 0~600）各取一次两边中间价，
  写入 `events.jsonl`（kind=`markout`）：成交价、成交时两边中间价、成交时价差 `edge0_bps`、半个盘口价差、
  `mo_maker{h}` / `mo_taker{h}`（单位 bps，**正数 = 对我们有利**）、是否美股常规交易时段、标的类别。
- open/close 记录里多了 `fills_n / m_mid_fill / t_mid_fill / fill_ts`，用来计算对冲成本（taker 实际成交价相对成交时 taker 中间价的滑点）。
- 新命令（只读，运行中也能跑）：

```bash
python -m arb markout --config config/xxx.yaml                 # 全部，按标的汇总
python -m arb markout --config config/xxx.yaml --by class      # 按 股票/加密币
python -m arb markout --config config/xxx.yaml --by session --hours 24   # 按 美股盘中/盘外，最近 24 小时
python -m arb markout --config config/xxx.yaml --symbols META,PLTR --by kind  # 开仓/平仓分开
```

- 输出按成交名义金额加权：`maker@h`（maker 那边中间价在 h 秒后相对成交价的走势）、`taker@h`、`hedge`（对冲滑点）、
  `net@h = maker@h − taker@h − hedge`（不含手续费）。`net@30 / net@60` 长期为负，说明这个标的上被动成交基本是被"挑单"，
  任何挂单模式都赚不到纯价差；为正且稳定的标的才值得考虑 `maker_price_mode: threshold` 之类的常驻挂单。
- 采样时任一边行情超过 5 秒没更新，会在该条记录的 `stale` 里标出对应周期（仍计入统计，用于排查断流）。

## v5.4：手续费从交易所 / 账户等级读取（不再靠手填）

- **Arcus**：启动时读 `/v1/markets` 里每个市场自己的费率表（加密币 Base 挂单 0.015% / 吃单 0.045%；股票等 RWA
  自定义表 挂单 0 / 吃单 0.0225%），再按 `/v1/account/stats?include=feeTier` 的账户等级取对应档位，正费率再扣账户
  折扣 `feeDiscountPpm`（返佣 5% 时 0.015% → 0.01425%）。读不到账户等级时按 Base（最高档，保守）。
- **Lighter / Lighter RH**：实盘启动时读 `/api/v1/accountLimits` 的账户等级和 `current_maker/taker_fee_tick`
  （单位 1e-6，Premium RH = 120 / 350 = 0.012% / 0.035%），应用到该账户所有市场。数值离谱（>1%）时不采用并告警。
- 每 `fee_refresh_s`（默认 3600 秒）重新读一次；费率变化会写日志。
- `venues.<名>.fee_source`：`auto`（默认，用交易所/账户费率；配置里的 `maker_fee_pct/taker_fee_pct` 只在读不到时兜底）
  或 `config`（强制用配置值，即旧行为）。Arcus 兜底默认改为 0.015 / 0.045。
- 开仓门槛 = T1 + 网格步长 + 往返手续费，所以加密币在 Arcus 上的开仓门槛会比以前高约 0.0285%（股票不变）。
- 新命令（只读，机器人运行时也可以跑）：`python -m arb fees --config config/xxx.yaml` 按标的列出机器人实际使用的费率和来源。
- 启动日志会打印每个交易所的费率分组；配置值和交易所不一致时给一条 WARNING（这是预期的，说明正在用交易所的值）。

## v5.3：小于最小下单额的网格不再反复刷单 / 提交时断线不再直接停机

实盘（服务器 A，PUMP）停机的根因及修复：

1. **网格价值跌到 taker 最小下单额以下**（PUMP 1807 张 ≈ 8.3U，Lighter 最小 10U）：旧版照常在 maker 挂平仓单 →
   成交后 taker 无法对冲（低于最小额）→ 网格不减、maker 多出一笔"平仓方向"残量 → 对账后又在 maker 把残量 IOC 平掉
   （等于重新开回去）→ 下一轮再平……来回刷单亏手续费和点差。v5.3：
   - taker 端（交易所最小数量 / 最小金额，留 5% 余量）接不住的平仓**不再挂单**，状态显示 `not closable`，并记一条警告；
     价格回升或网格被补满后自动恢复；
   - 平仓切片若会留下两边都下不了单的尾数，整格一次平掉；
   - 平仓对冲只看交易所最小额，不再受 `hedge_min_notional_usdc` 约束（平仓不会再有后续成交可以凑单）。
2. **平仓 maker 已成交、对冲没发出去**（平仓方向的 maker 残量）：不再在 maker 端平掉（那等于重新开仓），而是
   - 下一次平仓时一起对冲（maker 只挂还没平的部分，`carry_in` 带上已成交部分）；
   - 对账通过后直接在 taker 端只减仓 IOC 平掉对应网格（`events.jsonl` 里 `pair_off`）；数量太小 taker 接不住时就保持不动并显示
     `close hedge pending`，等下一次平仓一起处理。
3. **提交订单时连接被重置**（Arcus `Connection reset by peer`，拿不到 orderId）：旧版 8 秒后就停机，实际上 30 秒后已查明
   订单从未上簿。v5.3：
   - 执行器会一直按 clientId 查找，直到交易所能给出结论（Arcus：`max(30, unknown_lookup_s) + 5` 秒）；
   - Arcus 两个订单列表都查询成功且都没有该订单：挂单按"已撤、0 成交"处理（继续监听迟到成交，对账兜底），IOC 按拒单处理；
     查询本身失败不下结论；
   - 仍查不到结论才停机（与以前一样需要人工核对）。

无新增配置项，现有配置文件不用改。

## v5.2：成交不丢失（迟到成交 / 对冲期间成交 / 未上簿订单）

实盘两次停机的根因及修复：

1. **对冲期间挂单补齐成交被丢失**（OPENAI）：挂单部分成交 → 程序去 taker 对冲 → 对冲的几秒内挂单剩余部分也成交并结束 →
   旧版直接丢弃该订单、没读最后一笔成交，以为还剩数量，于是又挂了一张只减仓单（交易所仓位已为 0，被静默丢弃）→
   撤单时"exchange order index unknown"→ 停机。v5.2 丢弃订单前一定先读入最后成交。
2. **撤单推送先于成交推送**（ENA / Arcus）：Arcus 的订单状态与成交分两个频道推送，CANCELED 先到、成交后到，
   旧版收到撤单即认定 0 成交并停止跟踪，随后的成交被丢弃。v5.2：
   - 结束的订单继续接收推送 `late_fill_grace_s`（默认 120s），之后到达的成交是"迟到成交"，记为该边残量
     （`events.jsonl` 里 `late_fill`），由已有残量逻辑处理：同方向下一片一起对冲，或对账通过后在该交易所 IOC 平掉；
   - Arcus 撤掉的挂单在 2s / 8s 后再 REST 查一次（`verify_cancel_rest` / `verify_cancel_delays_s`）。
3. **从未上簿的挂单**（Lighter）：API 接受但从未出现在挂单/历史订单里，超过 `unknown_lookup_s`（默认 20s）按"已撤、0 成交"处理，
   不再停机（仍继续监听推送，对账兜底）。
4. **自动对齐（可选，默认关闭）**：`risk.auto_adopt_max_usdc` > 0 时，对账只有一边不符且连续两次差额相同、价值不超过上限，
   记为残量而不停机（`adopt_mismatch` 事件），每标的每小时最多 `auto_adopt_max_per_hour` 次；两边都不符、超额或不稳定仍停机。

## v5：全标的监控 / 自动加标的 / rank 打分 / 执行模式切换

### 1. 自动加入所有共同标的（`auto_symbols`，默认开启）

启动时读取四个交易所的公开市场列表（失败时用 `data/markets_cache.json` 缓存），把**至少两个交易所都上市**的标的自动加入，不用手动粘贴。`config.yaml` 里手写的标的保持原样不动。

当前实时数量（两边都有的标的）：lighter+arcus 57、lighter+lighter_rh 44、lighter_rh+arcus 37、lighter+entropy 8、lighter_rh/entropy+arcus 各 3。`monitor` 会同时监控全部组合（约 140+ 行）。

自动标的按类别套用参数模板（`auto_symbols.templates`，可改）：

| 类别 | 判定 | 默认参数 |
|---|---|---|
| `us_equity` 美股/ETF | 内置美股列表，或 Arcus 标注为 EQUITIES/INDICES/COMMODITIES | 盘内 T1 0.30%，**盘外 0.60%**（夜盘/周末/假日更谨慎） |
| `crypto` 加密 | 内置加密列表，或 Arcus 标注 CRYPTO | T1 0.20%，步长 0.10% |
| `other` 其它 | 未上市公司（OPENAI 等）、港韩股、外汇、贵金属、能源、利率、未知 | T1 0.40%，步长 0.20%（最保守） |

分类不对可以用 `auto_symbols.categories: {crypto: [ANSEM]}` 修正；不想要的标的放 `exclude`。美股想盘外完全不开仓：`strategy.offhours_open: false`（平仓照常）。

### 2. 监控全部，只交易筛选过的（`auto_symbols.trade`）

| 取值 | 自动标的能否开仓 |
|---|---|
| `none`（默认） | 只监控，不开仓（行情表显示“就绪·仅监控”） |
| `filtered` | 实时流动性过滤通过才开仓：两边盘口点差 ≤ `max_book_spread_pct`（0.15%）且最薄一侧深度 ≥ `min_depth_usdc`（300U），按约 60 秒 EMA 计算 |
| `ranked` | 只交易 `arb rank --save` 排名前 `ranked_top_n`（15）个，同时还要通过流动性过滤 |

- 手写的标的和 `include` 白名单里的标的照常交易（`filter_configured: true` 时也要过流动性过滤）。
- 只监控的标的不参与对账、不占用多开锁（不影响其他进程交易它）。
- 自动标的如果启动时交易所里已有**不是本程序开的仓位**（手动仓位或其他程序），会自动跳过该标的，不会因为对账不一致而拒绝启动。

### 3. `arb rank`：给标的打分

```bash
./start.sh monitor                       # 先全部监控 1–2 天（基差 EMA 需预热 1 小时后才有有效样本）
./start.sh rank                          # 按开仓机会打分
./start.sh rank --mode taker_taker       # 按 taker→taker 开仓模式打分（手续费按两边吃单）
./start.sh rank --pair lighter+arcus --top 20 --save   # 保存到 data/rank.json，供 trade: ranked 使用
```

每个标的输出：数据时长、每天机会次数、平均峰值超额（超过“T1 + 手续费 + 资金费”的部分）、平均持续秒数、机会时间占比、盘内占比（美股）、盘口点差中位数、深度中位数、分数。分数 ≈ 每天机会次数 × (T1 + 平均峰值超额) × 流动性系数，只用于排序，不是收益预测；数据不足 1 小时记 0。采样文件新增了 T1、两种模式手续费、资金费、taker→taker 价差、点差、深度、盘内标记等列（旧文件保留，新列写入 `YYYY-MM-DD_v5.csv`）。

### 4. 执行模式与 maker 方可用命令行切换

```bash
./start.sh live --venues lighter,arcus --maker lighter           # 临时改成在 Lighter 挂单、Arcus 吃单
./start.sh live --venues lighter,arcus --open-mode taker_taker   # 开仓两边都用 IOC 吃单
./start.sh live --venues lighter,arcus --close-mode taker_taker  # 平仓两边都吃单
./start.sh monitor --all-symbols                                 # 本次运行临时开启自动标的
```

- **taker→taker 开仓**（`execution.open_mode: taker_taker`）：信号按两边立即成交价（VWAP）计算，手续费按两边吃单费；满足条件时先在 maker 端 IOC，成交多少立刻在另一边对冲多少，不挂单等待。适合价差大、持续时间短的机会；手续费更高，T1 需要覆盖。
- 命令行参数优先于 `config.yaml`，对本次运行的所有标的生效；仪表盘“执行模式”一行显示当前模式。

### 5. 仪表盘

- 标题“套利执行终端 v5”；系统区显示执行模式、自动标的数量、可开仓数量。
- 行情表默认按“有持仓优先，其次离开仓门槛最近”排序（`ui.sort: gap`），每个标的一行；放不下的折叠成一行汇总（隐藏数量、就绪数、距开仓 ≤0.05% 的数量、最近差距）。`ui.max_book_rows` 可限制行数。
- 自动标的显示“·自动”；监控模式下“下次开”也会显示预估开仓门槛。

## v4：从 lighter-rh / arcus-maker-lighter-taker v8–v17 移植

以下功能来自 `baalisgood/lighter-rh`（arbitrage_monitor_v2 的全局风控 / 分段网格配置）和 `baalisgood/arcus-maker-lighter-taker`（v8–v17，main @18b02e4），改写为四交易所通用实现，原有功能与仪表盘全部保留。新代码集中在 `arb/engine/guards.py`，便于继续扩展。

**全局暂停（停止本实例所有新切片，不动仓位，到期自动恢复）** —— `guards:` 段，默认开启：

| 参数 | 默认 | 含义 |
|---|---|---|
| `slippage_pause_pct` / `slippage_pause_s` | 0.08 / 900 | 单笔实际 dev − 所需 dev ≤ −0.08% → 暂停 900s |
| `slippage_burst_count` / `slippage_burst_window_s` | 5 / 180 | 180s 内 5 笔滑点 ≤ −0.01% → 暂停 |
| `slippage_pause_scope` | all | `all` 整个实例 / `symbol` 只停该标的 |
| `hedge_latency_max_s` / `hedge_latency_pause_s` | 3.2 / 1000 | 对冲 IOC 从提交到终态超过 3.2s → 暂停 |
| `backoff_base_s` / `backoff_max_s` / `backoff_reset_s` | 10 / 300 / 600 | HTTP 429 / 限流 / nonce 错误 → 指数退避暂停（10→20→40…≤300s，600s 无错误重置） |
| `api_failure_pause`（默认关）/ `api_failure_count` / `_window_s` / `_pause_s` | false / 10 / 600 / 900 | 5xx / 超时 / 连接错误在窗口内达到次数 → 暂停 |

**只禁开仓（平仓照常）** —— `risk:` 段，可按标的覆盖：

| 参数 | 默认 | 含义 |
|---|---|---|
| `volatility_max_pct` / `volatility_window_s` / `volatility_block_s` | 4.0 / 300 / 900 | 300s 内中间价振幅 ≥ 4% → 禁开 900s |
| `reduce_only_block_s` | 1800 | 交易所以“只减仓”拒绝开仓 → 该标的禁开 1800s |
| `max_leg_spread_pct` | null（关） | taker 腿盘口点差超过此值不开仓 |
| `max_daily_opens` | null（关） | 每标的每 UTC 日最多开仓切片数 |
| `strategy.signal_confirm_s` | 0（关） | 开仓信号需连续保持 N 秒（价差持续性确认，join/improve 模式） |

**只提醒（事件日志 + 仪表盘“风险提醒” + 终端响铃 `bell`）**：两所权益差 ≥ `equity_gap_warn_usdc`（1000U，每 300s 提醒一次，资金不会自动划转）、单所权益 < `balance_warn_usdc`（默认关）、持仓超过 `risk.max_hold_hours`（默认关，不会自动平仓）、资金费率持续不利 ≥ `adverse_funding_warn_hours`（8h，净 ≥ 0.01%/h）。

**下单/订单状态安全（arcus-maker-lighter-taker v8–v17）**：

1. Lighter：签名阶段（尚未发送）抛出的异常判定为“未发送”→ 直接拒绝并强制刷新 nonce；HTTP 4xx（非 429）判定为拒绝并刷新 nonce；只有真正发出后结果不明才是 UNKNOWN，并继续查询最长 `venues.<名>.unknown_lookup_s`（20s）再决定是否 HALT。
2. Arcus：请求被拒时交易所推送的“零值 REJECTED”行（size 0 / market 0）只在订单仅有下单 ACK、无成交、未发撤单时才判定为订单被拒；否则（通常是撤单被拒、订单刚成交）不合并，改为 REST 重新读取真实状态。orderId / marketId 与本单矛盾的行（其他会话复用了 clientId）一律忽略并告警；零 size 的状态行不当作数量证据。
3. 所有交易所：限流 / nonce / 传输错误统一分类记录（`venue.failures`），驱动上面的退避暂停；残差修复会跳过处于退避中的交易所。
4. 美股交易时段：加入提前收盘日（感恩节次日、平安夜 13:00）；日历只维护到 2027 年，之后的年份一律按盘外（更高阈值）处理。
5. 启动对账不一致、滑点、对冲延迟、暂停/恢复都会写入 `events.jsonl`。

**仪表盘新增**：标题“套利执行终端 v4”；性能区“对冲延迟（p50/最大）”“成交滑点（近 20 笔均值/最差）”；风控区“暂停新单：原因（剩余 Ns）”和“风险提醒”；行情区状态显示“暂停 / 信号确认中 / 禁开原因 / ⚠提醒”；执行记录新增“滑点%”列。

**新命令**：

```bash
python -m arb markets [--symbols SNDK,MU] [--min-venues 2] [--yaml]   # 列出四所共同上市的标的，--yaml 输出可粘贴的 symbols 配置
python -m arb accept-flat --venues lighter,arcus [--confirm]          # 两边交易所确认已空仓后清空本地网格账本（默认只预览）
python -m arb report --chart [--symbols SNDK]                          # 价差/基差图 PNG 写入 data/charts/（需 pip install matplotlib，可选）
```

**未移植（说明原因）**：自动仓位对齐下单（自动补单风险大，本项目保持“HALT + 人工介入锁”）、低余额 / 最大亏损时强制平仓、WebSocket 批量下单、Lighter modify_order 改单、系统声音（改用终端响铃）、lighter-rh 的 100k 行监控框架本身（只取其风控规则与参数）。

## 监控面板（中文仪表盘）

`monitor` / `live` 在终端（TTY）下会全屏显示中文仪表盘，每秒刷新：

| 区域 | 内容 |
|---|---|
| 系统状态 | 模式（监控/实盘/只平）、交易对、运行时长、预热进度、启动时间 |
| 性能 | 各交易所 WS 帧率（帧/秒）、行情延迟、每次计算耗时（平均/最大）、并行执行切片数、健康告警次数 |
| 全局风险控制 | 状态（正常/暂停/HALT）、网络、两边余额与权益变化、策略毛利、未对冲名义、总名义/杠杆、开仓限制 / 人工干预锁 |
| 实时订单簿价格 + 资金费率 | 每个标的两边买一/卖一（含数量）、资金费率（%/h 与年化）、费率差年化、基差、偏离 dev、T1、下次开/平阈值、状态 |
| 套利对持仓明细 / 持仓校验 | 每个标的格数、maker/taker 均价、名义、浮盈；本地账本与交易所持仓对账结果（✓/✗） |
| 交易所总持仓 & 总价值 | 交易所实际持仓汇总与账户价值 |
| 执行记录 / 事件日志 | 最近成交（来自 `data/events.jsonl`，重启后保留）和最近事件 |

- 建议终端宽度 ≥ 200 列、高度 ≥ 40 行；窗口太小可用 `--no-ui`（纯文本滚动日志，适合 nohup / systemd / tmux 日志）。
- 非 TTY（重定向到文件）时自动使用纯文本模式。
- 持仓明细 / 持仓校验 / 交易所总持仓只在 `live` 模式、完成首次对账后才有数据。

## 目录

```
arb/
  cli.py            命令行
  config.py         配置解析与校验（两两规则、角色）
  venues/lighter.py Lighter 主网 + RH（同一实现，不同 URL/chain id/凭证前缀）
  venues/entropy.py Entropy / Hyperliquid HIP-3
  venues/arcus.py   Arcus（Ed25519 签名 REST + WebSocket）
  engine/claims.py  多进程之间的标的归属锁
  engine/basis.py   基差 EMA
  engine/signals.py 信号、网格线、VWAP
  venues/transport.py WebSocket 帧级心跳 / 拒绝重定向
  engine/executor.py maker→taker 执行、阈值挂单、增量对冲、碎单累积
  engine/state.py   网格仓位与残差（data/<交易对>/positions.json）
  engine/engine.py  主循环、决策、对账、采样
  engine/guards.py  v4 全局暂停 / 禁开 / 提醒（滑点、延迟、限流退避、波动…）
  markets.py        四所共同标的列表（arb markets）
  autosym.py        v5 自动加标的与分类模板
  ui.py / report.py 终端面板 / 价差统计
tests/              单元测试（pytest）
```

## 风险与未验证项（务必先小资金）

- 四个交易所的公共行情、价差计算、采样已对真实行情验证；**下单路径只在模拟交易所上测试过**，没有用真实凭证下过单。第一次实盘请把 `grid_value_usdc` / `order_value_usdc` 调到最小（≥ $10–15），盯盘观察。
- Arcus 下单/撤单的签名格式沿用原仓库 `lighter-arcus-stock-trading` 里已在用的实现（post-only = `ALO`，taker = `MARKET + IOC` 带保护价）。
- Lighter 的 taker 腿使用 `ORDER_TYPE_MARKET + IOC`（价格为最差成交价），与 SDK `create_market_order` 一致。
- Entropy 的 `orderUpdates` 推送在 HIP-3 上若有延迟，会自动退回 REST `orderStatus` 轮询。
- 两条 Lighter 链 / Hyperliquid 之间资金不能自动划转；某一边保证金不足时机器人只会停止开仓，需要手动调仓。
- 基差会漂移（尤其是 OPENAI/ANTHROPIC 这种盘前标的），`max_drift_pct` 会在基差快速变化时暂停交易。

```bash
pip install -r requirements-dev.txt && python -m pytest -q
```
