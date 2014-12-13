



class _Engine(object):
    """
    _LazyConnection WRAPS engine._connect
    and _DbContext WRAPS _LazyConnection
    why bother?
    """
    def __init__(self, connect):
        # connect is a function and is assigned to self._connect
        self._connect = connect
    
    def connect(self):
        # here _connect() is executed and the result is returned
        return self._connect()


class DbContext(threading.local):
    """
    This is a threading local object that holds information about
    connection and transactions
    
    _LazyConnection WRAPS engine._connect
    and _DbContext WRAPS _LazyConnection
    why bother?
    """
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        # todo
        # create a lazy connection object,
        #  it's not really initiated until cursor is called
        self.connection = _LazyConnection()
        self.transactions = 0

    def cleanup(self):
        # self.connection is a lazy connection object,
        #  call its cleanup method
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        """
        return the cursor
        """
        return self.connection.cursor()


# this is global
engine = None

# this is global
_db_context = DbContext()


def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    """
    create an engine and assign to global engine
    """
    import mysql.connector
    global engine
    if engine is not None:
        # todo DBError
        raise DBError('Engine is already initialized.')
    params = dict(user=user, password=password, database=database, host=host, port=port)
    defaults = dict(use_unicode=True, charset='utf8', collation='utf8_general_ci', autocommit=False)
    # use kw to update params, if any key is not in kw, use values in defaults 
    for k, v in defaults.iteritems():
        params[k] = kw.pop(k, v)
    # use the rest in kw to update params
    params.update(kw)
    params['buffered'] = True
    # the lambda function is passed to the _Engine object, then as _connect()
    engine = _Engine(lambda: mysql.connector.connect(**params))
    # to do

class _LazyConnection(object):
    """
    _LazyConnection WRAPS engine._connect
    and _DbContext WRAPS _LazyConnection
    why bother?
    """
    def __init__(self):
        # just declaire that there is self.connection,
        #  but not really initiate it
        self.connection = None

    def cursor(self):
        # this is where self.connection is really initiated
        if self.connection is None:
            # here engine.connect() returns engine._connect()
            # it's actually the lambda function in create_engine(),
            # which calls mysql.connector.connect(**params)
            connection = engine.connect()
            # todo
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            # todo
            connection.close()

class _ConnectionContext(object):
    # this class defines __enter__ and __exit__ so that
    # it can be used in 'with statement'
    def __enter__(self):
        # while entering, check global _db_context, init if necessary
        global _db_context
        self.should_cleanup = False
        if not _db_context.is_init():
            _db_context.init()
            self.should_cleanup = True

        return self

    def __exit__(self, exctype, excvalue, traceback):
        # while exiting, clean up global _db_context
        global _db_context
        if self.should_cleanup:
            _db_context.cleanup()

def connection():
    """
    just return a new _ConnectionContext object
    """
    return _ConnectionContext()


# connection() can be used by 'with statement', like this:
#   with connection():
#     do something...
#
# then it will be like:
#   __enter__()
#   then do something
#   __exit__()
# which means check, init, and clean up of _db_context will be
# done automatically for 'do something'

def with_connection(func):
    """
    Decorator for reuse connection. Instead of writing like this:
        with connection():
            do something...
    just write more conviniently like this:
        @with_connection
        def foo():
            do something...
 
    """
    @functools.wraps(func)
    def _wrapper(*args, **kw):
        # why bother define connection()?
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper



def _select(sql, first, *args):
    """
    execute select, return results
    return a Dict object
    """
    global _db_context
    cursor = None
    # replace all '?' in sql with '%s'
    sql = sql.replace('?', '%s')
    # to do
    try:
        cursor = _db_context.connection.cursor()
        # execute sql with args?
        cursor.execute(sql, args)
        # get column names
        if cursor.description:
            names = [x[0] for x in cursor.description]
        # if only select one row
        # But "You must fetch all rows for the current query
        # before executing new statements using the same connection."?
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, values)
        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def select_one(sql, *args):
    """
    select one row
    """
    return _select(sql, True, *args)

@with_connection
def select_int(sql, *args):
    """
    select one int, not multiple cols, not multiple rows
    """
    d = _select(sql, True, *args)
    if len(d) != 1:
        # to do
        raise MultipleColumnsError('Expect only one column.')
    return d.values()[0]

@with_connection
def select(sql, *args):
    return _select(sql, False, *args)

@with_connection
def _update(sql, *args):
    global _db_context
    cursor = None
    sql = sql.replace('?', '%s')
    # to do
    try:
        cursor = _db_context.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        # if no more transaction, then it's okay to commit
        if _db_context.transactions == 0:
            # to do
            _db_context.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def insert(table, **kw):
    # cols store keys, args store values
    cols, args = zip(*kw.iteritems())
    sql = 'insert into `%s` (%s) values (%s)' %(
        table,
        ','.join(['`%s`' %col for col in cols]),
        ','.join(['?' for i in range(len(cols))]))
    return _update(sql, *args)

def update(sql, *args):
    return _update(sql, *args)




class Dict(dict):
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

class DBError(Exception):
    pass

class MultiColumnsError(DBError):
    pass
