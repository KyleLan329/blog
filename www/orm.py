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

class Field(object):

    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):

    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

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

class Model(dict, metaclass = ModelMetaClass):
    def __init__(self, **kw):
        super(Model, self).__init__(self, **kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError('Model object has no attribute: %s' % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if not value:
            field = self.__mapping__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('use default value for %s: %s' % (key, str(value)))
            return value

    @classmethod
    async def findeAll(cls, where=None, args=None, **kw):
        if not args:
            args = []
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            else isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?', '?')
                args.extend(limit)
            else:
                raise('Invalid limit value: %s' % str(limit))
        rs = await select(''.join(sql), args)

        return [cls(**r) for r i rs]

    @classmethod
    async def find(cls, primarykey):
        sql = '%s where `%s`=?' % (cls.__select__, cls.__primary_key__)
        rs = await select(sql, [primarykey], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        sql = ['select %s __num__ from `%s`' % (selectField, cls.__table__)]
        if where: 
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['__num__']

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        primarykey = self.getValueOrDefault(self.__primary_key__)
        args.append(primarykey)
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warn('failed to insert record: affected rows: %s' % rows)

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        primarykey = self.getValue(self.__primary_key__)
        args.append(primarykey)
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warn('faild to update record: affected rows: %s' % rows)

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warn('failed to remove by primary key: affected rows: %s' % rows)

