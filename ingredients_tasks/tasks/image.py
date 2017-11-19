import celery
from celery.utils.log import get_task_logger

from ingredients_db.models.images import ImageState
from ingredients_db.models.region import Region
from ingredients_tasks.tasks.tasks import ImageTask
from ingredients_tasks.vmware import VMWareClient

logger = get_task_logger(__name__)


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def create_image(self, **kwargs):
    image = self.request.image
    region: Region = self.request.session.query(Region).filter(Region.id == image.region_id).one()
    with VMWareClient.client_session() as vmware:
        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))
        vmware_image = vmware.get_image(image.file_name, datacenter)

        if vmware_image is None:
            raise ValueError("Could not find image file")

    image.state = ImageState.CREATED


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def delete_image(self, **kwargs):
    image = self.request.image
    region: Region = self.request.session.query(Region).filter(Region.id == image.region_id).one()
    with VMWareClient.client_session() as vmware:
        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))
        vmware_image = vmware.get_image(image.file_name, datacenter)

        if vmware_image is not None:
            vmware.delete_image(vmware_image)
        else:
            logger.warning("Tried to delete image %s but couldn't find its backing file" % str(image.id))

    image.state = ImageState.DELETED

    self.request.session.delete(image)


@celery.shared_task(base=ImageTask, bind=True, max_retires=2, default_retry_delay=5)
def convert_vm(self, **kwargs):
    image = self.request.image
    region: Region = self.request.session.query(Region).filter(Region.id == image.region_id).one()
    with VMWareClient.client_session() as vmware:
        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))
        vmware_vm = vmware.get_vm(image.file_name, datacenter)

        if vmware_vm is None:
            raise LookupError(
                'Could not find backing VMWare VM for image %s when trying to convert to template.' % str(image.id))

        datastore = vmware.get_datastore(region.image_datastore, datacenter)
        if datastore is None:
            raise LookupError("Could not find VMWare datastore for region %s" % str(region.id))

        folder = None
        if region.image_folder is not None:
            folder = vmware.get_folder(region.image_folder, datacenter)
            if folder is None:
                raise LookupError("Could not find VMWare VM & Templates folder for region %s" % str(region.id))

        vmware.template_vm(vmware_vm, datastore, folder)

    image.state = ImageState.CREATED
