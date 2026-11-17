import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from backend.amqp import start_amqp_consumer
from backend.routers.iot import router as iot_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 配置了 AMQP_* 环境变量才会真正连华为云，否则只打一条告警
    start_amqp_consumer()
    yield


app = FastAPI(title="IoT Backend", lifespan=lifespan)
app.include_router(iot_router)


@app.get("/")
def read_root():
    return {"Hello": "World"}
