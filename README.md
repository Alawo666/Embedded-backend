# 床垫监测 IoT 系统（mattress-iot）

设备端通过华为云 IoTDA 上报温湿度、光照、三轴加速度、体动等属性，服务端消费数据并落库到 SQLite，提供 FastAPI 查询服务。

## 架构

```
STM32 / ESP32 设备
      │ MQTT (8883)
      ▼
华为云 IoTDA（设备接入）
      │ AMQP (5671，服务端主动连接)
      ▼
本服务 backend（FastAPI + 后台消费者线程）
      │ SQLite
      ▼
data/iot.db
```

- 设备与平台之间使用 MQTT，后端与平台之间使用 AMQP，后端**只需能出网访问 5671**，不需要公网 IP、域名或证书。
- 另保留一个 HTTP 推送接口 `POST /iot/callback`，用于本地联调和带签名校验的推送场景。

## 功能

- AMQP 订阅 IoTDA 属性上报，断线自动重连（1s 起、15s 封顶的指数退避）。
- 按服务解析并写入两张宽表：`EnvService → env_readings`，`BedService → bed_readings`。
- 类型自动转换：数值字符串、浮点整数都能正确入库，转换失败记日志并写 NULL。
- 懒建库：首次落库时自动创建目录和表；SQLite 开启 WAL，读写互不阻塞。
- 消息解析或写库失败只记录日志，不中断消费循环。
- 配置集中在 `.env`，真实凭证不进入 Git。

## 目录结构

```
项目根/
├── .env                  # 本地私有配置（git 忽略）
├── .env.example          # 配置模板
├── pyproject.toml
├── uv.lock
├── data/                 # 运行时生成（git 忽略）
│   └── iot.db
└── src/backend/
    ├── __init__.py
    ├── main.py           # FastAPI 入口，lifespan 中启动 AMQP 消费者
    ├── amqp.py           # AMQP 消费者、消息解析、重连
    ├── storage.py        # SQLite 建表与落库
    └── routers/
        └── iot.py        # POST /iot/callback（HTTP 推送 + 签名校验）
```

## 环境要求

- Python 3.14（由 `uv` 管理）
- [uv](https://docs.astral.sh/uv/)
- 依赖 `python-qpid-proton==0.39.0` 已锁定：0.40 之后不再提供 Windows 预编译包，安装会要求 MSVC 编译；0.39.0 自带 `cp38-abi3` wheel，Windows 可直接使用。

## 快速开始

```powershell
# 1. 安装依赖
uv sync

# 2. 准备配置
Copy-Item .env.example .env
# 编辑 .env，填入 IoTDA 接入凭证

# 3. 启动（任选其一）
uv run fastapi dev src/backend/main.py    # API + 后台 AMQP 消费者
uv run python -m backend.amqp             # 只跑消费者

# 4. 打开接口文档
# http://127.0.0.1:8000/docs
```

## 环境变量

| 变量 | 必填 | 说明 |
| --- | --- | --- |
| `AMQP_HOST` | 是 | IoTDA 实例的 AMQP 接入域名 |
| `AMQP_PORT` | 否 | 默认 `5671` |
| `AMQP_ACCESS_KEY` | 是 | IoTDA「接入凭证」的 AccessKey |
| `AMQP_ACCESS_CODE` | 是 | IoTDA「接入凭证」的 AccessCode |
| `AMQP_QUEUE` | 否 | 订阅队列名，默认 `DefaultQueue` |
| `AMQP_INSTANCE_ID` | 否 | 同 Region 多实例的标准版才填 |
| `SQLITE_PATH` | 否 | 默认项目根目录 `data/iot.db`，一般无需设置 |

优先级：**系统环境变量 > `.env`**，临时覆盖配置不会被文件锁死。缺少必填项时只告警并跳过订阅，不影响 API 启动。

## 华为云 IoTDA 配置

1. **获取接入凭证**：IoTDA 控制台 → 应用侧 → 接入凭证，创建后得到 AccessKey / AccessCode。
   - 注意：这里不是「我的凭证」里的 IAM AK/SK，两者不能混用。用错会报 `amqp:unauthorized-access`。
2. **获取接入域名**：实例 → 接入信息 → 应用接入 → AMQP 接入域名，端口 `5671`。
3. **创建 AMQP 订阅**：数据转发规则选择资源类型（如 `device.property`），转发目标指向 AMQP 队列，队列名与 `AMQP_QUEUE` 保持一致。
4. **设备侧**：设备通过 MQTT 上报属性，属性名需与产品模型中定义的一致。

## 数据模型

### env_readings（EnvService）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | INTEGER | 自增主键 |
| device_id | TEXT | 设备 ID |
| temperature | REAL | 温度 |
| humidity | REAL | 湿度 |
| light | INTEGER | 光照 |
| env_status | INTEGER | 环境状态 |
| event_time | TEXT | 平台事件时间 |
| received_at | TEXT | 本地接收时间（ISO 8601） |

### bed_readings（BedService）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| id | INTEGER | 自增主键 |
| device_id | TEXT | 设备 ID |
| accel_x / accel_y / accel_z | INTEGER | 三轴加速度原始值 |
| motion_power_peak | REAL | 近 10 秒运动功率峰值 |
| motion_state | TEXT | 床垫静止 / 运动状态 |
| motion_count_10s | INTEGER | 近 10 秒检测到的运动事件次数 |
| event_time | TEXT | 平台事件时间 |
| received_at | TEXT | 本地接收时间 |

属性名到列名的映射集中在 `storage.py` 的 `SERVICE_TABLES`，新增服务或属性只需改这一处。

### 查询示例

```sql
-- 最近 20 条温湿度
SELECT event_time, temperature, humidity
FROM env_readings
WHERE device_id = '你的设备ID'
ORDER BY id DESC
LIMIT 20;

-- 按小时统计温度均值
SELECT substr(event_time, 1, 13) AS hour, AVG(temperature)
FROM env_readings
GROUP BY hour
ORDER BY hour DESC;
```

## HTTP 接口

### GET /

健康检查，返回 `{"Hello": "World"}`。

### POST /iot/callback

接收 HTTP 推送，需携带三个请求头：`nonce`、`timestamp`、`signature`。

签名算法：将 `token`、`nonce`、`timestamp` 三个值按字典序排序后直接拼接，做 SHA-256 并转小写十六进制，与 `signature` 比对；不匹配返回 403。Token 在 `routers/iot.py` 的 `SUBSCRIBE_TOKEN` 中配置。

> 该接口仅用于本地联调或公网可达的场景；华为云 HTTP 推送要求公网 HTTPS 地址，内网服务请使用 AMQP 方式。

## 常见问题

**启动日志提示「跳过 AMQP 订阅：缺少环境变量」**
未配置 `AMQP_HOST / AMQP_ACCESS_KEY / AMQP_ACCESS_CODE`，检查 `.env` 是否存在、变量名是否正确。

**连接报 `amqp:unauthorized-access`**
凭证类型用错。必须使用 IoTDA 的「接入凭证」，不是 IAM 的 AK/SK；标准版多实例还需填 `AMQP_INSTANCE_ID`。

**收到推送日志，但库里没有数据**
查看是否有「未知服务，跳过落库」或「属性未映射」告警，确认产品模型中的服务名、属性名与 `SERVICE_TABLES` 一致。

**数据库文件在哪**
默认 `项目根/data/iot.db`，由代码中的绝对路径确定，与启动时所在目录无关。想换位置设置绝对路径的 `SQLITE_PATH`。

**平台重复推送导致重复数据**
IoTDA 在非 200 或超时时会重试。当前未做去重，可按 `device_id + event_time` 加唯一索引或写入前判重。

**后端在内网，华为云怎么推送**
HTTP 推送需要公网 HTTPS 地址；内网环境请使用 AMQP 主动订阅（本项目的默认方案）。

## 安全说明

- `.env` 已在 `.gitignore` 中，真实凭证不会提交；仓库只保留 `.env.example`。
- AMQP 当前使用 `ANONYMOUS_PEER`（不校验服务端证书），与官方示例一致；生产环境建议改为 `VERIFY_PEER` 并配置 CA。
- 接入凭证一旦泄露应立即在控制台删除并重新生成。

## 待办

- 推送幂等（按 `device_id + event_time` 去重）。
- 查询接口：按设备、时间范围、指标返回历史数据。
- 阈值告警与通知。
