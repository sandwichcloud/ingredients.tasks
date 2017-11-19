import os
import sys

import celery
from celery.utils.log import get_task_logger
from simple_settings import settings
from sqlalchemy.exc import OperationalError, IntegrityError, DataError, ProgrammingError
from sqlalchemy_utils.types.arrow import arrow

from ingredients_db.database import Database
from ingredients_db.models.images import ImageState, Image
from ingredients_db.models.instance import InstanceState, Instance
from ingredients_db.models.network import Network, NetworkState
from ingredients_db.models.region import RegionState, Region
from ingredients_db.models.task import TaskState, Task
from ingredients_db.models.zones import ZoneState, Zone

logger = get_task_logger(__name__)


class DBTask(celery.Task):
    def __init__(self):
        super().__init__()
        # TODO: find another place to put this db object (needs to not load the settings object)
        # Currently each task has a database connection with pool size of settings.DATABASE_POOL_SIZE
        # Cannot set to -1 pool size because some tasks need multiple connections
        self.database = Database(settings.DATABASE_HOST, settings.DATABASE_PORT, settings.DATABASE_USERNAME,
                                 settings.DATABASE_PASSWORD, settings.DATABASE_DB, settings.DATABASE_POOL_SIZE)
        self.database.connect()

    def __call__(self, *args, **kwargs):

        def commit_database(task, session):
            task.stopped_at = arrow.now()

            try:  # Try to commit, if error log and force quit
                session.flush()
            except (IntegrityError, DataError, ProgrammingError):
                logger.exception(
                    "Error flushing transaction to database. This is probably due to a bug somewhere")
                os.killpg(os.getpgrp(), 9)
                return
            session.commit()

        try:  # Try to do db stuff and catch OperationalError
            with self.database.session() as session:
                self.request.session = session
                task = session.query(Task).filter(Task.id == self.request.id).first()
                if task is None:  # We might be faster than the db so retry
                    raise self.retry()
                if task.stopped_at is not None:  # Task has already ran
                    raise ValueError("Task has already stopped, cannot do it again.")

                self.on_database(session)  # Load more stuff into the request

                try:
                    super().__call__(*args, **kwargs)
                    task.state = TaskState.COMPLETED
                except Exception as exc:  # There was an error during the call
                    try:  # Set task to error and reraise
                        task.state = TaskState.ERROR
                        task.error_message = str(exc.msg) if hasattr(exc, 'msg') else str(
                            exc)  # VMWare errors are stupid

                        self.on_task_failure()

                        raise exc
                    finally:  # Commit the error
                        commit_database(task, session)
                finally:  # Set task to stopped and commit
                    commit_database(task, session)

        except OperationalError:  # There some some sort of connection error so keep retrying
            raise self.retry(countdown=60, max_retries=sys.maxsize)

    def on_database(self, session):
        pass

    def on_task_failure(self):
        pass


class NetworkTask(DBTask):
    def on_database(self, session):
        self.request.network = None
        network = session.query(Network).filter(Network.id == self.request.kwargs['network_id']).first()
        if network is None:  # We might be faster than the db so retry
            raise self.retry()

        self.request.network = network

    def on_task_failure(self):
        if self.request.network is not None:
            self.request.network.state = NetworkState.ERROR


class ImageTask(DBTask):
    def on_database(self, session):
        self.request.image = None
        image = session.query(Image).filter(Image.id == self.request.kwargs['image_id']).first()
        if image is None:  # We might be faster than the db so retry
            raise self.retry()

        self.request.image = image

    def on_task_failure(self):
        if self.request.image is not None:
            self.request.image.state = ImageState.ERROR


class InstanceTask(DBTask):
    def on_database(self, session):
        self.request.instance = None
        instance = session.query(Instance).filter(Instance.id == self.request.kwargs['instance_id']).first()
        if instance is None:  # We might be faster than the db so retry
            raise self.retry()

        self.request.instance = instance

    def on_task_failure(self):
        if self.request.instance is not None:
            self.request.instance.state = InstanceState.ERROR


class RegionTask(DBTask):
    def on_database(self, session):
        self.request.region = None
        region = session.query(Region).filter(Region.id == self.request.kwargs['region_id']).first()
        if region is None:  # We might be faster than the db so retry
            raise self.retry()

        self.request.region = region

    def on_task_failure(self):
        if self.request.region is not None:
            self.request.region.state = RegionState.ERROR


class ZoneTask(DBTask):
    def on_database(self, session):
        self.request.zone = None
        zone = session.query(Zone).filter(Zone.id == self.request.kwargs['zone_id']).first()
        if zone is None:  # We might be faster than the db so retry
            raise self.retry()

        self.request.zone = zone

    def on_task_failure(self):
        if self.request.zone is not None:
            self.request.zone.state = ZoneState.ERROR


def create_task(session, entity, celery_task, signature=False, **kwargs):
    task = Task()
    task.name = celery_task.__name__
    task.state = TaskState.PENDING
    session.add(task)
    session.flush()

    entity.current_task_id = task.id

    logger.info("Sending Task: " + celery_task.__name__ + " with ID " + str(task.id))

    if signature is False:
        celery_task.apply_async(kwargs=kwargs, task_id=str(task.id))
    else:
        celery_task.s(kwargs=kwargs, task_id=str(task.id))
