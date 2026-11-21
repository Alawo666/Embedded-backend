import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from backend.amqp import start_amqp_consumer
from backend.routers.api import router as api_router
from backend.routers.iot import router as iot_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger("iot.main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 配置了 AMQP_* 环境变量才会真正连华为云，否则只打一条告警
    start_amqp_consumer()
    yield


app = FastAPI(title="IoT Backend", lifespan=lifespan)
app.include_router(iot_router)
app.include_router(api_router)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """把 HTTPException 统一成 {code, message, data}。"""
    return JSONResponse(
        status_code=exc.status_code,
        content={"code": exc.status_code, "message": str(exc.detail), "data": None},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """参数校验失败统一返回 400 与可读提示。"""
    errors = exc.errors()
    if errors:
        first = errors[0]
        location = ".".join(str(part) for part in first.get("loc", ()))
        message = f"参数校验失败: {location} {first.get('msg', '')}".strip()
    else:
        message = "参数校验失败"
    return JSONResponse(
        status_code=400,
        content={"code": 400, "message": message, "data": None},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """兜底异常：记录堆栈到后端日志，返回统一的 500 响应体。

    注意：debug=True 时 Starlette 会直接返回调试页，本处理器不生效，
    部署时必须保持 debug=False。
    """
    logger.exception("未处理异常: %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"code": 500, "message": "服务器内部错误", "data": None},
    )
