import celery
from celery.utils.log import get_task_logger

from ingredients_db.models.images import ImageState
from ingredients_tasks.tasks.tasks import ImageTask
from ingredients_tasks.vmware import VMWareClient

logger = get_task_logger(__name__)


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def create_image(self, **kwargs):
    image = self.request.image
    with VMWareClient.client_session() as vmware:
        vmware_image = vmware.get_image(image.file_name)

        if vmware_image is None:
            raise ValueError("Could not find image file")

    image.state = ImageState.CREATED


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def delete_image(self, **kwargs):
    image = self.request.image
    with VMWareClient.client_session() as vmware:
        vmware_image = vmware.get_image(image.file_name)

        if vmware_image is not None:
            vmware.delete_image(vmware_image)
        else:
            logger.warning("Tried to delete image %s but couldn't find its backing file" % str(image.id))

    image.state = ImageState.DELETED

    self.request.session.delete(image)


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def convert_vm(self, **kwargs):
    image = self.request.image
    with VMWareClient.client_session() as vmware:
        vmware_vm = vmware.get_vm(image.file_name)

        if vmware_vm is None:
            raise LookupError(
                'Could not find backing vm for image %s when trying to convert to template.' % str(image.id))

        vmware.template_vm(vmware_vm)

    image.state = ImageState.CREATED
