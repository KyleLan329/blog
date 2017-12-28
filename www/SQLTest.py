# -*- coding: utf-8 -*-

import sys
import orm, asyncio

from models import User, Blog, Comment

async def test(loop):
    await orm.create_pool(loop, user='lantern', password='lanternblog', db='awesome')
    u = User(name='Kyle', email='kylelan329@qq.com', passwd='123456', image='about:blank')
    await u.save()

if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    loop.run_until_complete( asyncio.wait([test(loop)]) )  
    loop.close()
    if loop.is_closed():
        sys.exit(0)