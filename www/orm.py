import aiomysql, asyncio, logging
from fields import Field
logging.basicConfig(level=logging.INFO)

#Print SQL
def log(sql, args=()):
    logging.info('SQL: %s,%s' % (sql,args))

#Create a global connect pool
async def create_pool(loop, **kw):
    logging 