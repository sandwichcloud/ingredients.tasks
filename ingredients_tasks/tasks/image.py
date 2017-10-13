import celery
from celery.utils.log import get_task_logger

from ingredients_db.models.images import ImageState
from ingredients_tasks.tasks.tasks import ImageTask

logger = get_task_logger(__name__)


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def create_image(self, **kwargs):
    vmware_image = self.vmware_session.get_image(self.image.file_name)

    if vmware_image is None:
        raise ValueError("Could not find image file")

    self.image.state = ImageState.CREATED


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def delete_image(self, **kwargs):
    vmware_image = self.vmware_session.get_image(self.image.file_name)

    if vmware_image is not None:
        self.vmware_session.delete_image(vmware_image)
    else:
        logger.warning("Tried to delete image %s but couldn't find its backing file" % self.image.id)

    self.image.state = ImageState.DELETED

    self.db_session.delete(self.image)


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def convert_vm(self, **kwargs):
    vmware_vm = self.vmware_session.get_vm(self.image.file_name)

    if vmware_vm is None:
        raise LookupError(
            'Could not find backing vm for image %s when trying to convert to template.' % str(self.instance.id))

    self.vmware_session.template_vm(vmware_vm)

    self.image.state = ImageState.CREATED
