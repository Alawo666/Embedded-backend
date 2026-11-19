"""SQLite 落库：把设备属性上报拆成一条条指标存下来。

存储位置默认是项目根目录的 data/iot.db，可用环境变量 SQLITE_PATH 覆盖。
"""

import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger("iot.storage")

_DEFAULT_DB = Path(__file__).resolve().parents[2] / "data" / "iot.db"


def _db_path() -> Path:
    """每次取路径，保证 .env / 环境变量在导入之后再加载也能生效。"""
    return Path(os.getenv("SQLITE_PATH", str(_DEFAULT_DB))).expanduser()


SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id   TEXT    NOT NULL,
    service_id  TEXT,
    property    TEXT    NOT NULL,
    value_text  TEXT,
    value_num   REAL,
    event_time  TEXT,
    received_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_readings_device_property
    ON readings (device_id, property, received_at);
"""

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
        with _connect() as conn:
            conn.executescript(SCHEMA)
        _initialized = True
        logger.info("SQLite 就绪: %s", db_path)


def save_readings(records: list[dict]) -> int:
    """写入读数，返回写入条数。"""
    if not records:
        return 0

    init_db()
    now = datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")
    rows = [
        (
            r["device_id"],
            r.get("service_id"),
            r["property"],
            r.get("value_text"),
            r.get("value_num"),
            r.get("event_time") or now,
            now,
        )
        for r in records
    ]

    with _connect() as conn:
        conn.executemany(
            "INSERT INTO readings"
            " (device_id, service_id, property, value_text, value_num, event_time, received_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    logger.info("已写入 %d 条读数", len(rows))
    return len(rows)
