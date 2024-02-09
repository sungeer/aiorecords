# aiorecords
用于fastapi的异步原生SQL连接池及工具集

# 初始化
from todoist.utils import db


@app.on_event("startup")
async def startup():
    await db.connect()


@app.on_event("shutdown")
async def shutdown():
    await db.disconnect()

