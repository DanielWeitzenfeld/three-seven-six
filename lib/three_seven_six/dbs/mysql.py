import os
import datetime
from contextlib import contextmanager
import yaml
import urlparse
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from pandas import DataFrame
import three_seven_six
import time

SLEEP_SECONDS = 2
BATCH_SIZE = 10000


@contextmanager
def session():
    """Provide a transactional scope around a series of operations."""
    session = scoped_session(Session)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def read_configuration():
    if 'THREE_SEVEN_SIX_MYSQL' in os.environ:
        url = urlparse.urlparse(os.environ['THREE_SEVEN_SIX_MYSQL'])
        return {
            three_seven_six.APP_ENV: {
                'host': url.hostname,
                'port': url.port or 3306,
                'user': url.username,
                'pass': url.password,
                'database': url.path[1:],
                'echo': False,
            }
        }
    else:
        with open(three_seven_six.ROOT + '/etc/database.yml') as file:
            return yaml.load(file)


def make_engine(environment):
    config = configuration[environment]
    if 'port' in config and 'pass' in config:
        url = 'mysql://{user}:{pass}@{host}:{port}/{database}'.format(**config)
    elif 'pass' in config:
        url = 'mysql://{user}:{pass}@{host}/{database}'.format(**config)
    else:
        url = 'mysql://{user}@{host}/{database}'.format(**config)
    echo = bool(config['echo']) if 'echo' in config else False
    return create_engine(url, echo=echo)


def execute(session, sql, args=None, df=True):
    """Executes query, returns results as a pandas dataframe, returns connection to pool"""
    global engine
    if args:
        results = session.connection().execute(sql, args)
    else:
        results = session.connection().execute(sql)
    if results.cursor and df:
        columns = [r[0] for r in results.cursor.description]
        return DataFrame(list(results), columns=columns) if results.rowcount else DataFrame(columns=columns)
    elif results.cursor:
        return [row for row in results]


class query(object):
    def __init__(self, base_query, where_text=''):
        self.base_query = base_query
        self.where_text = where_text

    @property
    def query(self):
        return self.base_query.replace('WHERE_TEXT', self.where_text)

    def execute(self, session, args=None):
        global execute
        return execute(session, self.query, args)


def init():
    global engine
    global Session
    global configuration
    configuration = read_configuration()
    engine = make_engine(three_seven_six.APP_ENV)
    Session = sessionmaker(bind=engine)


init()


def create_col_list(L, primary_key=None):
    'list of tuples containing column_name, dtype'
    cols = []
    for key, value in L:
        pair = " ".join([key, value])
        cols.append(pair)
    if primary_key:
        cols.append(' PRIMARY KEY (%s) ' % primary_key)
    return ",".join(cols)


def drop_table(session, name):
    sql = 'drop table if  exists %s;'
    execute(session, sql % name)

def create_table(session, name, col_list):
    sql = 'create table if not exists %s (%s)'
    execute(session, sql % (name, col_list))


def create_index(session, name, table_name, col_list):
    sql = 'create index %s on %s (%s);' % (name, table_name, col_list)
    execute(session, sql)


def update_table_one_col(session, table, array, column_name_tuple):
    column_string = array_of_tuples_to_string([column_name_tuple], False)
    data_string = iterable_to_myql_array_string(array)
    update = update_string(column_name_tuple)
    sql = """INSERT into %s %s VALUES %s
                ON DUPLICATE KEY UPDATE %s""" % (table, column_string, data_string, update)
    #try_executing(sql)
    execute(session, sql)


def update_table(session, table, array_of_tuples, column_name_tuple):
    column_string = array_of_tuples_to_string([column_name_tuple], False)
    data_string = array_of_tuples_to_string(array_of_tuples)
    update = update_string(column_name_tuple)
    sql = """INSERT into %s %s VALUES %s
                ON DUPLICATE KEY UPDATE %s""" % (table, column_string, data_string, update)
    #try_executing(sql)
    execute(session, sql)


def stringify_for_mysql(x, quote_strings=True):
    if isinstance(x, pd.tslib.NaTType):
        return 'NULL'
    elif isinstance(x, datetime.datetime) or isinstance(x, pd.tslib.Timestamp):
        return """'""" + x.strftime('%Y-%m-%d %H:%M:%S') + """'"""
    elif (isinstance(x, unicode) or isinstance(x, str)) and quote_strings:
        x = x.replace("'", "''")
        return """'""" + x + """'"""
    elif isinstance(x, float) and np.isnan(x):
        return 'NULL'
    return 'NULL' if x is None else str(x)


def iterable_to_myql_array_string(iterable, quote_strings=True):
    return '(' + ','.join((stringify_for_mysql(row, quote_strings) for row in iterable)) + ')'


def array_of_tuples_to_string(array_of_tuples, quote_strings=True):
    return ','.join([iterable_to_myql_array_string(row, quote_strings) for row in array_of_tuples])


def batch_update_table(session, table, array_of_tuples, column_name_tuple):
    total_size = len(array_of_tuples)
    batch_size = BATCH_SIZE
    for i in range(0, int(total_size / batch_size) + 1):
        this_batch = array_of_tuples[i * batch_size: (i + 1) * batch_size]
        update_table(session, table, this_batch, column_name_tuple)
        time.sleep(SLEEP_SECONDS)
    session.commit()


def update_string(tuple_of_columns):
    return_me = []
    for col in tuple_of_columns:
        return_me.append('%s = VALUES(%s)' % (col, col))
    return ', '.join(return_me)

