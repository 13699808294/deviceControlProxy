import asyncio
import uvloop
from tornado.platform.asyncio import BaseAsyncIOLoop
from app.websocketService import WebsocketServer

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # 修改循环策略为uvloop
    aioloop = asyncio.get_event_loop()  # 获取aioloop循环事件
    ioloop = BaseAsyncIOLoop(aioloop)  # 使用aioloop创建ioloop

    WebsocketServer(aioloop=aioloop,ioloop=ioloop)
    ioloop.current().start()