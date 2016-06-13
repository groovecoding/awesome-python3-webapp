import asyncio
import logging
import aiomysql


@asyncio.coroutine
def create_pool(loop, **kwargs):
    logging.info('create database connection pool')
    global __pool
    __pool = yield from aiomysql.create_pool(
        host=kwargs.get('host', 'localhost'),
        port=kwargs.get('port', 3306),
        user=kwargs['password'],
        db=kwargs['db'],
        charset=kwargs.get('charset', 'utf8'),
        autocommit=kwargs.get(('autocommit', True)),
        maxsize=kwargs.get('maxsize', 10),
        minsize=kwargs.get('minsize', 1),
        loop=loop
    )


@asyncio.coroutine
def select(sql, args, size=None):
    logging.log(sql, args)
    global __pool
    with (yield from __pool) as conn:
        cur = yield from conn.cursor(aiomysql.DictCursor)
        yield from cur.execute(sql.replace('?', '%s'). args or ())
        if size:
            rs = yield from cur.fetchmany(size)
        else:
            rs = yield from cur.fetchall()
        yield from cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs
