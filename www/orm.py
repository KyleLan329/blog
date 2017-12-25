import aiomysql, asyncio, logging
from fields import Field
logging.basicConfig(level=logging.INFO)

#Print SQL
def log(sql, args=()):
    logging.info('SQL: %s,%s' % (sql,args))

#Create a global connect pool
async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host = kw.get('host', 'localhost'),
        port = kw.get('port', 3306),
        user = kw['user'],
        password = kw['password'],
        db = kw['db'],
        charset = kw.get('charset','utf-8'),
        autocommit = kw.get('autocommit', True),
        maxsize = kw.get('maxsize', 10),
        minsize = kw.get('minsize', 1),
        loop = loop)

#Package select function
async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', "%s"), args or ())
            if size:
                rs =  await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
    logging.info('rows returned: %s' % len(rs))
    return rs

#Package insert, update, delete function
async def execute(sql, args, autocommit=True):
    log(sql, args)
    with await __pool as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', "%s"), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException as e:
            if not autocommit:
                await conn.rollback()
            raise e
        return affected

#Create placeholder for insert, update, delete
def create_args_string(num):
    L = []
    for i in range(num):
        L.append('?')
    return ','.join(L)

#Define Metaclass
class ModelMetaclass(type):
    def __new__(cls, name bases, attrs):
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        tableName = attrs.get('__table__', None) or name
        logging.info('found model: %s' % (table: %s) %(name, tableName))
        mappings = dict()
        fields = []
        primarykey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ==> %s' % (k,v))
                mappings[k] = v
                if v.primary_key:
                    if primarykey:
                        raise BaseException('Duplicate primary key for field: %s' % k)
                    primarykey = k
                else:
                    fields.append(k)
        if not primarykey:
            raise BaseException('primary key not found')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: '`%s`' %f, fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tableName
        attrs['__fields'] = fields
        attrs['__primary_key__'] = primarykey
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primarykey, ','.join(escaped_fields), tableName)
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ','.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primarykey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primarykey)
        return type.__new__(cls, name, bases, attrs)

