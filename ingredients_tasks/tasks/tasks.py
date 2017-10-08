import os
import sys

import celery
from celery.utils.log import get_task_logger
from sqlalchemy.exc import OperationalError, IntegrityError, DataError, ProgrammingError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy_utils.types.arrow import arrow

from ingredients_db.models.images import Image, ImageState
from ingredients_db.models.instance import InstanceState, Instance
from ingredients_db.models.network import Network, NetworkState
from ingredients_db.models.task import TaskState, Task
from ingredients_tasks.celary import database
from ingredients_tasks.omapi import OmapiClient
from ingredients_tasks.vmware import VMWareClient

logger = get_task_logger(__name__)


class BaseMixin(object):
    def __init__(self):
        pass


class DatabaseMixin(BaseMixin):
    def __init__(self):
        super().__init__()
        self.db_session_manager = None
        self.db_session = None

    def setup_db_session(self):
        self.db_session_manager = database.session()
        self.db_session = self.db_session_manager.__enter__()

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        if self.db_session is not None:
            try:
                self.db_session.flush()
            except (IntegrityError, DataError, ProgrammingError):
                logger.exception("Error flushing transaction to database. This is probably due to a bug somewhere")
                os.killpg(os.getpgrp(), 9)
            self.db_session.commit()
        self.db_session_manager.__exit__(None, None, None)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        if isinstance(exc, OperationalError):
            # Rerun the task again in 60 seconds
            self.retry(countdown=60, max_retries=sys.maxsize, throw=False)


class VMWareMixin(BaseMixin):
    def __init__(self):
        super().__init__()
        self.vmware_session_manager = None
        self.vmware_session = None

    def setup_vmware_session(self):
        self.vmware_session_manager = VMWareClient.client_session()
        self.vmware_session = self.vmware_session_manager.__enter__()

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        self.vmware_session_manager.__exit__(None, None, None)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # We shouldn't retry vmware errors, just let it fail.
        pass


class OmapiMixin(BaseMixin):
    def __init__(self):
        super().__init__()
        self.omapi_session_manager = None
        self.omapi_session = None

    def setup_omapi_session(self):
        self.omapi_session_manager = OmapiClient.client_session()
        self.omapi_session = self.omapi_session_manager.__enter__()

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        self.omapi_session_manager.__exit__(None, None, None)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # We shouldn't retry omapi errors, just let it fail.
        pass


class TaskStateMixin(DatabaseMixin):
    def __init__(self):
        super().__init__()
        self.task = None

    def setup_task(self, task_id, kwargs):
        self.setup_db_session()
        try:
            self.task = self.db_session.query(Task).filter(Task.id == task_id).with_for_update().one()
        except NoResultFound as exc:  # We might be faster than the db so retry
            raise self.retry()

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        if self.task is not None:
            self.task.stopped_at = arrow.now()
        super().after_return(status, retval, task_id, args, kwargs, einfo)

    def on_success(self, retval, task_id, args, kwargs):
        self.task.state = TaskState.COMPLETED

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)
        if self.task is not None:
            if hasattr(exc, 'msg'):  # VMWare errors are stupid
                self.task.error_message = exc.msg
            else:
                self.task.error_message = str(exc)
            self.task.state = TaskState.ERROR


class ImageTask(TaskStateMixin, VMWareMixin, celery.Task):
    def __init__(self):
        super().__init__()
        self.image = None

    def __call__(self, *args, **kwargs):
        self.setup_task(self.request.id, kwargs)
        try:
            self.image = self.db_session.query(Image).filter(Image.id == kwargs['image_id']).with_for_update().one()
        except NoResultFound as exc:  # We might be faster than the db so retry
            raise self.retry()
        self.setup_vmware_session()
        super().__call__(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        super().after_return(status, retval, task_id, args, kwargs, einfo)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)
        if self.image is not None:
            self.image.state = ImageState.ERROR


class InstanceTask(TaskStateMixin, VMWareMixin, OmapiMixin, celery.Task):
    def __init__(self):
        super().__init__()
        self.instance = None

    def __call__(self, *args, **kwargs):
        self.setup_task(self.request.id, kwargs)
        try:
            self.instance = self.db_session.query(Instance).filter(
                Instance.id == kwargs['instance_id']).with_for_update().one()
        except NoResultFound as exc:  # We might be faster than the db so retry
            raise self.retry()
        self.setup_vmware_session()
        self.setup_omapi_session()
        super().__call__(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        super().after_return(status, retval, task_id, args, kwargs, einfo)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)
        if self.instance is not None:
            self.instance.state = InstanceState.ERROR


class NetworkTask(TaskStateMixin, VMWareMixin, celery.Task):
    def __init__(self):
        super().__init__()
        self.network = None

    def __call__(self, *args, **kwargs):
        self.setup_task(self.request.id, kwargs)
        try:
            self.network = self.db_session.query(Network).filter(
                Network.id == kwargs['network_id']).with_for_update().one()
        except NoResultFound as exc:  # We might be faster than the db so retry
            raise self.retry()
        self.setup_vmware_session()
        super().__call__(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        super().after_return(status, retval, task_id, args, kwargs, einfo)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        super().on_failure(exc, task_id, args, kwargs, einfo)
        if self.network is not None:
            self.network.state = NetworkState.ERROR


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