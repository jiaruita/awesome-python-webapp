



class _Engine(object):

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

class _LazyConnection(object):
    """
    _LazyConnection wraps engine._connect
    and _DbContext wraps _LazyConnection
    """
    def __init__(self):
        # just declaire that there is self.connection,
        #  but not really initiate it
        self.connection = None

    def cursor(self):
        # this is where self.connection is really initiated
        if self.connection is None:
            # here engine.connect() returns engine._connect()
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
    def __enter__(self):
        global _db_context
        self.should_cleanup = False
        if not _db_context.is_init():
            _db_context.init()
            self.should_cleanup = True

        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_context
        if self.should_cleanup:
            _db_context.cleanup()

def connection():
    return _ConnectionContext()
