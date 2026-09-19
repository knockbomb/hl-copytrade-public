# HL CopyTrade · 跟单助手（公开版）
> Hyperliquid 合约自动跟单系统：自动监控 Leader（Leader）账户仓位变化，并同步到你的账户执行相同买卖。
> 上次同步：2026-09-19
> 本仓库为**脱敏公开版**，去除了所有个人敏感信息，供学习与自行搭建使用。

---

# 🚀 一、这是做什么的？
简单说：**别人买什么，你就跟着买什么，完全自动。**

1. 程序持续监控一个 **Leader 账户**（Leader）在 Hyperliquid 上的合约仓位。
2. 当 Leader **新开仓 / 加仓 / 减仓 / 平仓** 时，程序自动在你的账户上执行**同样的操作**。
3. 跟单比例、杠杆、止损止盈等都可以配置。

> ⚠️ 使用前必读：合约交易风险极高，可能损失全部本金。**请先用小额资金测试**，充分理解后再考虑加大投入。

---

# 📦 二、你需要准备什么
### 1️⃣ 硬件与系统
- 一台 **Linux 服务器 / VPS**（推荐 Ubuntu 20.04/22.04），或你的电脑（安装 Ubuntu/WSL）
- 需要**能长期稳定运行**（建议 VPS，24小时在线）
- Python 3.8+

### 2️⃣ 两个 Hyperliquid 账户
| 角色 | 是什么 | 在哪创建 |
|------|--------|---------|
| **Leader（Leader）地址** | 你要跟单的那个人的钱包地址 | 他人提供 |
| **你的主钱包** | 你的资金账户，持仓在这里 | `app.hyperliquid.xyz` 登录即可 |
| **你的 API 钱包** | 专供程序下单用的子钱包（Agent Wallet） | HL官网 → **API** 创建 |

### 3️⃣ 创建 API 钱包（重点！）
1. 登录 `app.hyperliquid.xyz`
2. 点击 **API** / **Agent Wallets** 标签
3. 点击 **Create New API**
4. **权限**：勾选 **Trading**（必须，用于下单）
5. **有效期**：选 **MAX（180天）**
6. 创建后**立即保存**：
   - API 钱包地址（`Agent Address`）
   - API 钱包私钥（`Private Key`，0x开头的66位字符串）—— **只显示一次，务必保密！**
7. 往主钱包 **充值 USDC**，作为跟单保证金（这是你的资金池）

> ❗ API 钱包私钥 = 你账户的钥匙，**绝不能泄露给任何人**。

---

# ⚙️ 三、安装部署（分步操作）

### 第 1 步：获取代码
在服务器终端执行：
```bash
git clone https://github.com/knockbomb/hl-copytrade-public.git
cd hl-copytrade-public
```

### 第 2 步：创建虚拟环境（隔离依赖）
```bash
python3 -m venv venv
source venv/bin/activate
```
> 看到命令行开头出现 `(venv)` 就说明激活成功。

### 第 3 步：安装依赖
```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 第 4 步：配置环境变量
```bash
cp .env.example .env
vim .env        # 或者 nano .env
```
把 `.env` 里的占位符改成你的真实信息（必填 4 项）：
| 变量 | 填什么 | 示例 |
|------|--------|------|
| `HL_LEADER_ADDR` | Leader（被跟单者）的主钱包地址 | `0x3Db...e5ef` |
| `HL_USER_MAIN_ADDR` | 你的主钱包地址 | `0xf745...D5B` |
| `HL_USER_API_ADDR` | 你的 API 钱包地址 | `0xAa1...9d` |
| `HL_API_PK` | 你的 API 钱包**私钥** | `0xaaa...` |

保存退出（vim: 按 `Esc` → 输入 `:wq` → 回车）。

### 第 5 步：检查配置文件
程序默认配置在 `config_v4.yaml`。小白通常**不需要改**，直接能用。如果以后想调跟单比例，改这里：
```yaml
instances:
  highfreq:
    fund_ratio: 0.5   # 跟单比例：0.5 = 按Leader仓位的50%跟进
```
> 注意：`fund_ratio` 只对**新开/加仓**生效，已有仓位不追溯调整。

### 第 6 步：启动跟单
```bash
# 先小金额试跑（推荐）：不实际下单，只观察程序是否正常
python3 hl_copytrade_v3.py

# 确认正常后，真正下单：
python3 hl_copytrade_v3.py --live
```

---

# 🧪 四、如何确认它正常在工作？
启动后观察窗口输出，应能看到类似：
```
[INFO] [WS] 已连接 wss://api.hyperliquid.xyz/ws        ← 连接交易所成功
[INFO] [WS] 已订阅主DEX openOrders                     ← 正在监控订单
[INFO] [RECON] 对账完成，无偏差                        ← 持仓一致
[INFO] 跟单比例: 账户总值×0.5 / Leader账户总值             ← 配置生效
```
- 有 `[WS] 已连接` → 连接正常
- 有 `[RECON] 无偏差` → 跟单对齐正常
- Leader开仓、你账户出现**相同币种仓位** → 跟单成功 🎉

> 💡 首次启动请用**小额**（比如100U）测试，确认跟单方向、比例正确后再加大。

---

# 🔧 五、常见问题（小白排错手册）

### ❓ Q1: 启动报 `未找到API私钥`
→ **原因**：`.env` 没填或填错 `HL_API_PK`
→ **解决**：确认 `.env` 里 `HL_API_PK=0x` 开头、共66位

### ❓ Q2: 报 `未配置HL_LEADER_ADDR`
→ **原因**：`.env` 里 `HL_LEADER_ADDR` 没填
→ **解决**：把Leader的主钱包地址贴进去

### ❓ Q3: 提示 API 钱包过期 / 认证失败
→ **原因**：API钱包有效期到了，或权限没勾 Trading
→ **解决**：到 HL 官网重新创建 API 钱包（选 MAX 180天），更新 `.env` 后重启

### ❓ Q4: 连接 websocket 失败 / 断连
→ **原因**：交易所服务端波动（常见），程序会自动重连
→ **解决**：无需操作，等待自动恢复；长期反复可检查服务器网络

### ❓ Q5: 跟单不下去 / 报保证金不足
→ **原因**：你账户 USDC 或保证金不够
→ **解决**：往主钱包充值 USDC，或调小 `fund_ratio`

### ❓ Q6: 怎么停止跟单？
→ 按 `Ctrl+C` 停止程序。已有持仓**不会**自动平掉，需手动在 HL 平仓。

---

# 📁 六、目录结构说明
```
├── hl_copytrade_v3.py      # 核心跟单引擎（主程序）
├── hl_orchestrator.py      # 多实例进程管理器（可同时跑多个账户）
├── hl_phase2/3/4_monitor.py# 监控与告警模块
├── hl_alert_manager.py     # 告警管理
├── unified_alert_queue.py  # 告警队列
├── shared_config.py        # 公共配置
├── paths.py                # 路径配置
├── config_v4.yaml          # 交易主配置
├── key_events_logger.py    # 关键事件日志
├── net_value_tracker.py    # 净值跟踪
├── generate_report.py      # 报表生成
├── generate_charts.py      # 图表
├── collect_data.py         # 数据采集
├── .env.example            # 环境变量模板（复制为.env填写）
├── install.sh              # 一键安装脚本
├── hl_copytrade_v3/        # 核心引擎子模块
└── wallet_manager/         # API钱包管理
```

---

# ⚠️ 七、风险声明
- 本程序仅供**学习与研究**，不构成任何投资建议。
- 加密合约交易**风险极高**，可能损失全部本金。
- 使用前请**完整理解风险**，务必**从小额开始测试**。
- 请勿在未充分测试前投入大额资金。

---

## License
MIT
