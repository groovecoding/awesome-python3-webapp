import logging
from www.dboperation import select


def log(sql, args=None):
    logging.info('SQL: [%s] args: %s' % (sql, str(args or [])))


class Field:
    def __init__(self, name, column_type, primary_key, default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)


class StringField(Field):
    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
        super(StringField, self).__init__(name, ddl, primary_key, default)


class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super(IntegerField, self).__init__(name, 'bigint', primary_key, default)


class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super(FloatField, self).__init__(name, 'real', primary_key, default)


class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super(BooleanField, self).__init__(name, 'boolean', False, default)


class TextField(Field):
    def __init__(self, name=None, default=False):
        super(TextField, self).__init__(name, 'text', False, default)


class ModelMetaclass(type):
    def __new__(mcs, name, bases, attrs):

        if name == 'Model':   # 排除掉Model本身，为什么？ 因为model不是表
            return super(ModelMetaclass, mcs).__new__(mcs, name, bases, attrs)

        table_name = attrs.get('__table__', None) or name
        logging.info('found model: %s (table: %s)' % (name, table_name))

        mappings = dict()
        fields = []
        primary_key = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info('found mapping: %s ===> %s' % (k, v))
                mappings[k] = v
                if v.primary_key:  # find primary
                    if primary_key:
                        raise RuntimeError('Duplicated pri key for field %s' % k)
                    primary_key = k
                else:
                    fields.append(k)
        if not primary_key:
            raise RuntimeError('P K not found')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_field = list(map(lambda f: '`%s`' % f, fields))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = table_name
        attrs['__primary_key__'] = primary_key
        attrs['__fields__'] = fields
        # construct select, insert and ,update, delete
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primary_key, ','.join(escaped_field), table_name)
        attrs['__insert__'] = 'insert into `%s`(%s) values (%s)' % (table_name,
                                                                    ','.join('`%s`' % f for f in mappings),
                                                                    ','.join('?' * len(mappings))
                                                                    )
        attrs['__update__'] = 'update `s` set %s where `%s` = ?' % (table_name,
                                                                    ','.join('`%s` = ?' % f for f in escaped_field))
        attrs['__delete__'] = 'delete from `%s`  where `%s` = ?' % (table_name, primary_key)

        return super(ModelMetaclass, mcs).__new__(mcs, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kwargs):
        super(Model, self).__init__(**kwargs)

    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(r'Model obj has no attribute "%s"' % attr)

    def __setattr__(self, attr, value):
        self[attr] = value

    def get_value_or_default(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s:%s' % (key, str(value)))
                setattr(self, key, value)

    @classmethod
    async def findAll(cls, where=None, args=None, **kwargs):
        sql = [cls.__select__]
        if args is None:
            args = []
        if where:
            sql.append('where %s' % where)

        if kwargs.get('orderBy') is not None:
            sql.append('order by %s' % (kwargs['orderBy']))

        limit = kwargs.get('limit')
        if limit is not None:
            if isinstance(limit, int):
                sql.append('limit ?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('limit ?, ?')
                args.extend(limit)
            else:
                raise ValueError
        result_set = await select(' '.join(sql), args)
        return result_set
