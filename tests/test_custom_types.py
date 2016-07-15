import asyncio
import psycopg2
import pytest
sa = pytest.importorskip("aiopg.sa")  # noqa

import sqlalchemy.types as types
from enum import Enum

from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.schema import CreateTable, DropTable


class PythonMappedEnum(types.TypeDecorator):
    """ Implements mapping between Postgres' Enums and Python Enums.
    """
    impl = types.Integer

    def __init__(self, python_enum_type: Enum, **kwargs):
        self.python_enum_type = python_enum_type
        self.kwargs = kwargs
        super().__init__(**self.kwargs)

    def process_bind_param(self, value: Enum, dialect):
        """ Convert to postgres value
        """
        return value.value

    def process_result_value(self, value: str, dialect):
        """ Convert to python value
        """
        for __, case in self.python_enum_type.__members__.items():
            if case.value == value:
                return case
        raise TypeError("Cannot map INT value '{}' to Python's {}".format(
            value, self.python_enum_type
        ))

    def copy(self):
        return self.__class__(self.python_enum_type, **self.kwargs)


meta = MetaData()


class Color(Enum):
    red = 1
    green = 2


tbl = Table('sa_custom_types', meta,
            Column('id', Integer, nullable=False,
                   primary_key=True),
            Column('val', PythonMappedEnum(Color)))


@pytest.yield_fixture
def connect(make_engine):
    @asyncio.coroutine
    def go(**kwargs):
        engine = yield from make_engine(**kwargs)
        with (yield from engine) as conn:
            try:
                yield from conn.execute(DropTable(tbl))
            except psycopg2.ProgrammingError:
                pass
            yield from conn.execute(CreateTable(tbl))
        return engine

    yield go


@pytest.mark.run_loop
def test_custom_type(connect):
    engine = yield from connect()
    with (yield from engine) as conn:
        yield from conn.execute(tbl.insert().values(val=Color.red))
        import ipdb;ipdb.set_trace()
        ret = yield from conn.execute(tbl.select())
        item = yield from ret.fetchone()
        assert item.val == Color.red
