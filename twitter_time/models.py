

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

    # TODO: Move to base class.
    @classmethod
    def add_index(cls, *cols, **kwargs):
        """Add an index to the table.
        """
        # Make slug from column names.
        col_names = '_'.join([c.name for c in cols])

        # Build the index name.
        name = 'idx_{}_{}'.format(cls.__tablename__, col_names)

        idx = Index(name, *cols, **kwargs)

        # Render the index.
        try:
            idx.create(bind=engine)
        except Exception as e:
            print(e)

        print(col_names)

    @classmethod
    def token_series(cls, token, corpus=None, pos=None):
        """Get an offset -> count series for a word.

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
