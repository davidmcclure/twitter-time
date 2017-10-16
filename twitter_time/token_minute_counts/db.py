

import os
import numpy as np
import glob
import ujson
import click

from collections import OrderedDict

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine, event, Column, Integer, String, func
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base


db_path = os.path.join(os.path.dirname(__file__), 'data.db')
url = URL(drivername='sqlite', database=db_path)
engine = create_engine(url)
factory = sessionmaker(bind=engine)
session = scoped_session(factory)


Base = declarative_base()
Base.query = session.query_property()


class TokenMinuteCount(Base):

    __tablename__ = 'token_minute_count'

    __table_args__ = dict(sqlite_autoincrement=True)

    id = Column(Integer, primary_key=True)

    token = Column(String, nullable=False, index=True)

    minute = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)

    @classmethod
    def load(cls, pattern):
        """Bulk-insert rows from JSON partitions.
        """
        for path in glob.glob(pattern):
            with open(path) as fh:

                segment = [ujson.loads(line) for line in fh]
                session.bulk_insert_mappings(cls, segment)

                session.commit()
                print(path)

    @classmethod
    def overall_series(cls):
        """Get overall minute -> word count totals.

        Args:
            token (str)

        Returns: np.array
        """
        query = (
            session
            .query(cls.minute, func.sum(cls.count))
            .group_by(cls.minute)
            .order_by(cls.minute)
        )

        series = np.zeros(60)

        for minute, count in query:
            series[minute] = count

        return series

    @classmethod
    def token_series(cls, token):
        """Get an minute -> count series for a word.

        Args:
            token (str)

        Returns: np.array
        """
        query = (
            session
            .query(cls.minute, func.sum(cls.count))
            .filter(cls.token == token)
            .group_by(cls.minute)
            .order_by(cls.minute)
        )

        series = np.zeros(60)

        for minute, count in query:
            series[minute] = count

        return series

    @classmethod
    def token_series_omit_5s(cls, token):
        """Token series with mod-5 minutes.

        Args:
            token (str)

        Returns: np.array
        """
        series = cls.token_series(token)

        counts = [count for i, count in enumerate(series) if i % 5 != 0]

        return np.array(counts)

    @classmethod
    def token_counts(cls):
        """Get total (un-bucketed) token counts.

        Args:
            min_count (int)

        Returns: OrderedDict
        """
        query = (
            session
            .query(cls.token, func.sum(cls.count))
            .group_by(cls.token)
            .order_by(func.sum(cls.count).desc())
        )

        return OrderedDict(query.all())


@click.group()
def cli():
    pass


@cli.command()
def create():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


@cli.command()
@click.argument('path', type=click.Path())
def load(path):
    TokenMinuteCount.load(path)


if __name__ == '__main__':
    cli()
