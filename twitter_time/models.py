

import numpy as np
import ujson

from datetime import datetime as dt
from collections import OrderedDict

from sqlalchemy import Column, Integer, String
from sqlalchemy.schema import Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func

from .utils import scan_paths
from .db import session, engine


Base = declarative_base()
Base.query = session.query_property()


class MinuteCount(Base):

    __tablename__ = 'minute_count'

    __table_args__ = dict(sqlite_autoincrement=True)

    id = Column(Integer, primary_key=True)

    token = Column(String, nullable=False)

    minute = Column(Integer, nullable=False)

    count = Column(Integer, nullable=False)

    @classmethod
    def load(cls, root):
        """Bulk-insert rows from CSVs.
        """
        for path in scan_paths(root, '\.json$'):
            with open(path) as fh:

                segment = [ujson.loads(line) for line in fh]
                session.bulk_insert_mappings(cls, segment)

                session.commit()
                print(dt.now(), path)

    @classmethod
    def token_series(cls, token):
        """Get an minute -> count series for a word.

        Args:
            token (str)

        Returns: OrderedDict
        """
        query = (
            session
            .query(cls.minute, func.sum(cls.count))
            .filter(cls.token == token)
            .group_by(cls.minute)
            .order_by(cls.minute)
        )

        series = np.zeros(60)

        for offset, count in query:
            series[offset] = count

        return series

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
