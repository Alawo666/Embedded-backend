"""SQLite 落库：按服务拆成宽表。

EnvService -> env_readings  (temperature / humidity / light / env_status)
BedService -> bed_readings  (accel_x / accel_y / accel_z / motion_power_peak / motion_state / motion_count_10s)

存储位置默认是项目根目录的 data/iot.db，可用环境变量 SQLITE_PATH 覆盖。
"""

import logging
import os
import sqlite3
import threading
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("iot.storage")

_DEFAULT_DB = Path(__file__).resolve().parents[2] / "data" / "iot.db"


def _db_path() -> Path:
    """每次取路径，保证 .env / 环境变量在导入之后再加载也能生效。"""
    return Path(os.getenv("SQLITE_PATH", str(_DEFAULT_DB))).expanduser()


SCHEMA = """
CREATE TABLE IF NOT EXISTS env_readings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT    NOT NULL,
    temperature REAL,
    humidity    REAL,
    light       INTEGER,
    env_status  INTEGER,
    event_time  TEXT,
    received_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_env_device_time
    ON env_readings (device_id, received_at);

CREATE TABLE IF NOT EXISTS bed_readings (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id         TEXT    NOT NULL,
    accel_x           INTEGER,
    accel_y           INTEGER,
    accel_z           INTEGER,
    motion_power_peak REAL,
    motion_state      TEXT,
    motion_count_10s  INTEGER,
    event_time        TEXT,
    received_at       TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bed_device_time
    ON bed_readings (device_id, received_at);
"""


def _to_int(value):
    return int(float(value))


# 服务名 -> (表名, {属性名: (列名, 类型转换)})
SERVICE_TABLES = {
    "EnvService": (
        "env_readings",
        {
            "temperature": ("temperature", float),
            "humidity": ("humidity", float),
            "light": ("light", _to_int),
            "env_status": ("env_status", _to_int),
        },
    ),
    "BedService": (
        "bed_readings",
        {
            "accelX": ("accel_x", _to_int),
            "accelY": ("accel_y", _to_int),
            "accelZ": ("accel_z", _to_int),
            "motionPowerPeak": ("motion_power_peak", float),
            "motionState": ("motion_state", str),
            "motionCount10s": ("motion_count_10s", _to_int),
        },
    ),
}

_init_lock = threading.Lock()
_initialized = False


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(), timeout=5)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        db_path = _db_path()
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with closing(_connect()) as conn, conn:
            conn.executescript(SCHEMA)
        _initialized = True
        logger.info("SQLite 就绪: %s", db_path)


def _convert(value, conv):
    if value is None:
        return None
    try:
        return conv(value)
    except (TypeError, ValueError):
        logger.warning("属性值 %r 无法转换为 %s，存 NULL", value, conv.__name__)
        return None


def save_services(device_id: str, services: list[dict], event_time: str | None = None) -> int:
    """把一条推送里的各服务属性写入对应宽表，返回写入行数。"""
    if not device_id or not services:
        return 0

    init_db()
    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")

    pending = []
    for service in services:
        service_id = service.get("service_id")
        mapping = SERVICE_TABLES.get(service_id)
        if mapping is None:
            logger.warning("未知服务 %s，跳过落库: %s", service_id, service.get("properties"))
            continue

        table, columns = mapping
        properties = service.get("properties") or {}
        unknown = set(properties) - set(columns)
        if unknown:
            logger.warning("服务 %s 有未映射属性，已忽略: %s", service_id, ", ".join(sorted(unknown)))

        row = {
            col: _convert(properties.get(prop), conv)
            for prop, (col, conv) in columns.items()
        }
        row["device_id"] = device_id
        row["event_time"] = service.get("event_time") or event_time or now
        row["received_at"] = now
        pending.append((table, row))

    if not pending:
        return 0

    with closing(_connect()) as conn, conn:
        for table, row in pending:
            placeholders = ", ".join("?" for _ in row)
            conn.execute(
                f"INSERT INTO {table} ({', '.join(row)}) VALUES ({placeholders})",
                list(row.values()),
            )
    logger.info("已写入 %d 条服务记录", len(pending))
    return len(pending)
