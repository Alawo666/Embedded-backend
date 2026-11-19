"""华为云 IoTDA AMQP 订阅消费者

依赖版本说明：
    python-qpid-proton 锁定 0.39.0 —— 0.40.0 之后没有 Windows 的预编译包，
    安装会要求 MSVC 编译；0.39.0 提供 cp38-abi3 的 wheel，Windows 直接可用。

两种运行方式：
    1. 随 FastAPI 一起启动（backend.main 的 lifespan 里调用 start_amqp_consumer）
    2. 单独进程：uv run python -m backend.amqp

需要的环境变量（PowerShell 示例）：
    $env:AMQP_HOST="xxxxx.iotda-app.myhuaweicloud.com"
    $env:AMQP_ACCESS_KEY="你的 AccessKey"
    $env:AMQP_ACCESS_CODE="你的 AccessCode"
    $env:AMQP_QUEUE="DefaultQueue"          # 选填
    $env:AMQP_INSTANCE_ID=""                # 标准版多实例才填
    $env:SQLITE_PATH="data/iot.db"          # 选填，默认项目根目录 data/iot.db
"""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv
from proton import SSLDomain
from proton.handlers import MessagingHandler
from proton.reactor import Container

from backend.storage import save_services

logger = logging.getLogger("iot.amqp")

# 项目根目录的 .env；已存在的系统环境变量优先，不会被覆盖
load_dotenv(Path(__file__).resolve().parents[2] / ".env")


@dataclass
class AmqpConfig:
    host: str
    access_key: str
    access_code: str
    queue: str = "DefaultQueue"
    instance_id: str = ""
    port: int = 5671

    @classmethod
    def from_env(cls) -> "AmqpConfig":
        host = os.getenv("AMQP_HOST", "").strip()
        access_key = os.getenv("AMQP_ACCESS_KEY", "").strip()
        access_code = os.getenv("AMQP_ACCESS_CODE", "").strip()

        missing = [
            name
            for name, value in (
                ("AMQP_HOST", host),
                ("AMQP_ACCESS_KEY", access_key),
                ("AMQP_ACCESS_CODE", access_code),
            )
            if not value
        ]
        if missing:
            raise SystemExit("缺少环境变量: " + ", ".join(missing))

        return cls(
            host=host,
            access_key=access_key,
            access_code=access_code,
            queue=os.getenv("AMQP_QUEUE", "DefaultQueue").strip() or "DefaultQueue",
            instance_id=os.getenv("AMQP_INSTANCE_ID", "").strip(),
            port=int(os.getenv("AMQP_PORT", "5671")),
        )


def handle_message(body) -> None:
    """处理一条推送，body 通常是 UTF-8 的 JSON 字节串。"""
    if isinstance(body, (bytes, bytearray)):
        body = body.decode("utf-8", errors="replace")

    if isinstance(body, str):
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            logger.warning("非 JSON 消息，原样输出: %.500s", body)
            return
    else:
        data = body

    notify = data.get("notify_data") or {}
    header = notify.get("header") or {}
    device_id = data.get("device_id") or header.get("device_id")

    logger.info(
        "收到推送 resource=%s event=%s device=%s",
        data.get("resource"),
        data.get("event"),
        device_id,
    )

    # 温湿度等属性：notify_data.body.services[].properties
    services = (notify.get("body") or {}).get("services") or []
    if not services and isinstance(data.get("payload"), dict):
        services = [{"service_id": None, "properties": data["payload"]}]

    if not services:
        logger.info("未解析到 services，原始消息: %.1000s", json.dumps(data, ensure_ascii=False))
        return

    for service in services:
        logger.info(
            "service=%s properties=%s",
            service.get("service_id"),
            service.get("properties"),
        )

    if not device_id:
        logger.warning("消息缺少 device_id，跳过落库")
        return

    try:
        save_services(device_id, services, data.get("event_time"))
    except Exception:
        logger.exception("写入 SQLite 失败")
    # TODO: 幂等。平台可能重复推送，需要按 device_id + event_time 去重。


class IotAmqpClient(MessagingHandler):
    def __init__(self, config: AmqpConfig):
        super().__init__()
        self.config = config

    def on_start(self, event):
        url = f"amqps://{self.config.host}:{self.config.port}"
        timestamp = str(int(time.time() * 1000))
        user = (
            f"accessKey={self.config.access_key}|timestamp={timestamp}"
            f"|instanceId={self.config.instance_id}"
        )

        ssl_domain = SSLDomain(SSLDomain.MODE_CLIENT)
        # 与官方示例一致：不校验服务端证书。生产环境建议改为 VERIFY_PEER 并配置 CA。
        ssl_domain.set_peer_authentication(SSLDomain.ANONYMOUS_PEER)

        conn = event.container.connect(
            url,
            user=user,
            password=self.config.access_code,
            heartbeat=60,
            ssl_domain=ssl_domain,
        )
        event.container.create_receiver(conn, source=self.config.queue)
        logger.info("正在连接 %s，队列=%s", url, self.config.queue)

    def on_connection_opened(self, event):
        logger.info("AMQP 连接已建立")

    def on_message(self, event):
        try:
            handle_message(event.message.body)
        except Exception:
            logger.exception("消息处理失败")


def run_forever(config: AmqpConfig) -> None:
    """断线后退避重连，直到进程被终止。"""
    delay = 1
    while True:
        started = time.monotonic()
        try:
            Container(IotAmqpClient(config)).run()
            logger.warning("AMQP 连接已断开")
        except Exception:
            logger.exception("AMQP 连接异常")

        # 长时间保持连接后断开，说明网络/服务端重启，快速重试即可；
        # 秒断说明是鉴权或配置错误，逐步拉长间隔避免被限流。
        if time.monotonic() - started > 30:
            delay = 1
        logger.info("%s 秒后重连", delay)
        time.sleep(delay)
        delay = min(delay * 2, 15)


_thread: threading.Thread | None = None


def start_amqp_consumer() -> None:
    """启动后台订阅线程；缺少配置时只告警，不影响 API 启动。"""
    global _thread
    if _thread is not None and _thread.is_alive():
        return

    try:
        config = AmqpConfig.from_env()
    except SystemExit as exc:
        logger.warning("跳过 AMQP 订阅：%s", exc)
        return

    _thread = threading.Thread(
        target=run_forever,
        args=(config,),
        name="iot-amqp",
        daemon=True,
    )
    _thread.start()
    logger.info("AMQP 订阅线程已启动")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        run_forever(AmqpConfig.from_env())
    except KeyboardInterrupt:
        logger.info("已手动退出")
