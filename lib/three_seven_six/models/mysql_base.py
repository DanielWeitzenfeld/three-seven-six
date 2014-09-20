from sqlalchemy.ext.declarative import declarative_base
from three_seven_six.dbs import mysql as mysql_db
from pprint import pprint
import datetime


class Augmentation(object):
    @classmethod
    def find(klass, *args):
        return list(mysql_db.session.query(klass).filter(*args).all())

    @classmethod
    def count(klass, *args):
        return mysql_db.session.query(klass).filter(*args).count()

    def save(self):
        with mysql_db.session() as session:
            session.add(self)
            session.commit()
        return self

    def pprint(self, columns=None):
        print('\n')
        if columns:
            for c in columns:
                print '%s: %s' % (c, getattr(self, c))
        else:
            pprint(self.to_dict())

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    def set_attr_from_dict(self, adict):
        for c in self.__table__.columns:
            if c.name in adict:
                setattr(self, c.name, adict[c.name])

    @classmethod
    def bulk_upsert_from_df_records(cls, session, columns, data, batch_size=1000):
        table = cls.__tablename__
        updates = ','.join(["{0} = VALUES({0})".format(c) for c in columns])
        columns_sql = ','.join(columns)
        for batch in batch_gen(data, batch_size):
            values = ["({0})".format(','.join(map(str, row))) for row in batch]
            values_sql = ','.join(values)
            sql = """
              INSERT INTO {table} ({columns}) VALUES {values}
              ON DUPLICATE KEY UPDATE {updates}
             """.format(table=table, columns=columns_sql, values=values_sql, updates=updates)
            session.execute(sql)

    @classmethod
    def bulk_insert_from_objects(cls, session, data, batch_size=5000):
        table = cls.__tablename__
        columns = [c.name for c in cls.__table__.columns]
        # import pdb; pdb.set_trace()
        columns_sql = ','.join(columns)
        for batch in batch_gen(data, batch_size):
            values = [stringify_object(row, columns) for row in batch]
            values_sql = ','.join(values)
            sql = """
              INSERT INTO {table} ({columns}) VALUES {values}
             """.format(table=table, columns=columns_sql, values=values_sql)
            session.execute(sql)

    @classmethod
    def bulk_insert_from_dicts(cls, session, data, batch_size=5000):
        table = cls.__tablename__
        columns = [c.name for c in cls.__table__.columns]
        columns_sql = ','.join(columns)
        for batch in batch_gen(data, batch_size):
            values = [stringify_dict(row, columns) for row in batch]
            values_sql = ','.join(values)
            sql = """
              INSERT INTO {table} ({columns}) VALUES {values}
             """.format(table=table, columns=columns_sql, values=values_sql)
            session.execute(sql)


def time_sql(time):
    return "'%s'" % time.strftime('%Y-%m-%d %H:%M:%S')


def stringify(x):
    if x is None:
        return "NULL"
    if isinstance(x, datetime.datetime):
        return time_sql(x)
    return "'%s'" % x


def stringify_dict(d, cols):
    ordered_strings = [stringify(d.get(c)) for c in cols]
    return "({0})".format(','.join(ordered_strings))


def stringify_object(o, cols):
    ordered_strings = [stringify(getattr(o, c)) for c in cols]
    return "({0})".format(','.join(ordered_strings))


def batch_gen(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def stringify_iterable(iterable):
    return "(" + ",".join(stringify(x) for x in iterable) + ")"


MysqlBase = declarative_base(cls=Augmentation)
