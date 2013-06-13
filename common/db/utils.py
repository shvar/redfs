#!/usr/bin/python
"""
Various SQLAlchemy-related extras.
"""

#
# Imports
#
from __future__ import absolute_import

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import column
from sqlalchemy.sql.expression import ClauseElement, Executable, FromClause



#
# Classes
#

class InsertFromSelect(Executable, ClauseElement):
    r"""Support for INSERT FROM SELECT construct.

    >>> # InsertFromSelect(t1, select([t1]).where(t1.c.x>5))

    @note: source from
        http://codereview.stackexchange.com/questions/18978/
            sqlalchemy-insertfromselect-with-columns-support
    """
    def __init__(self, insert_spec, select):
        self.insert_spec = insert_spec
        self.select = select


@compiles(InsertFromSelect, 'postgresql')
def visit_insert_from_select(element, compiler, **kw):

    if isinstance(element.insert_spec, list):
        assert element.insert_spec, repr(element.insert_spec)
        base_table = element.insert_spec[0].table
        columns = []
        for column in element.insert_spec:
            if column.table != base_table:
                raise Exception('Insert columns must belong to the same table')
            columns.append(column)

        sql = u'INSERT INTO {} ({}) {}'.format(
                  compiler.process(base_table, asfrom=True),
                  ', '.join(u'"{}"'.format(c.name)
                                for c in columns),
                  compiler.process(element.select))

    else:
        sql = u'INSERT INTO {} ({})'.format(
                  compiler.process(element.insert_spec, asfrom=True),
                  compiler.process(element.select))

    return sql


def insert_from_select(*args, **kwargs):
    """Create INSERT FROM SELECT (or alike) query.

    Use as:

        insert_from_select([dst_table.c.col2, dst_table.c.col1],
                           select([src_table.c.col1, src_table.c.col1]))

    or:

        insert_from_select(dst_table, select(src_table]))
    """
    return InsertFromSelect(*args, **kwargs)



# This code uses repr() to render the raw values into the SQL code,
# thus it is pretty unsafe.
# class Values(FromClause):
#     def __init__(self, *args):
#         self.list = args

#     def _populate_column_collection(self):
#         data = (('column{:d}'.format(i),
#                  column('column{:d}'.format(i)))
#                     for i in xrange(1, len(self.list[0]) + 1))
#         self._columns.update(data)


# def values(*args):
#     """Create VALUES query with raw data.
#     """
#     return Values(*args)


# @compiles(values)
# def compile_values(element, compiler, asfrom=False, **kw):
#     """
#     @todo: using repr() is not very safe...
#     """
#     values_rows = ('({})'.format(', '.join(repr(elem)
#                                                for elem in tup)
#                                      for tup in element.list))
#     values_contents = ', '.join(values_rows)
#     v = u'VALUES {}'.format(values_contents)
#     return u'({})'.format(v) if asfrom else v


# if __name__ == '__main__':
#     from sqlalchemy.sql import table, column
#     t1 = table('t1', column('a'), column('b'))
#     t2 = values((1, 0.5), (2, -0.5)).alias('weights')
#     print select([t1, t2]).select_from(t1.join(t2, t1.c.a==t2.c.column2))
