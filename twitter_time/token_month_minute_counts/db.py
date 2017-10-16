

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


class TokenMonthMinuteCount(Base):

    __tablename__ = 'token_month_minute_count'

    __table_args__ = dict(sqlite_autoincrement=True)

    id = Column(Integer, primary_key=True)

    token = Column(String, nullable=False, index=True)

    month = Column(Integer, nullable=False, index=True)

    minute = Column(Integer, nullable=False, index=True)

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
    def token_series(cls, token):
        """Get an minute -> count series for a word.

        Args:
            token (str)

        Returns: np.array
        """
        query = (
            session
            .query(cls.month, cls.minute, func.sum(cls.count))
            .filter(cls.token == token)
            .group_by(cls.month, cls.minute)
            .order_by(cls.month, cls.minute)
        )

        return query.all()


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
    TokenMonthMinuteCount.load(path)


if __name__ == '__main__':
    cli()
