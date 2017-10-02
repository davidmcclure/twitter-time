

from invoke import task

from twitter_time.db import engine
from twitter_time.models import Base, MinuteCount


@task
def reset_db(ctx):
    """Recreate the bin counts database.
    """
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


@task
def index_db(ctx):
    """Create indexes on bin counts database.
    """
    MinuteCount.add_indexes()
