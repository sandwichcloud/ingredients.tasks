import celery
from celery.utils.log import get_task_logger

from ingredients_db.models.region import RegionState
from ingredients_tasks.tasks.tasks import RegionTask
from ingredients_tasks.vmware import VMWareClient

logger = get_task_logger(__name__)


@celery.shared_task(base=RegionTask, bind=True, max_retires=2, default_retry_delay=5)
def create_region(self, **kwargs):
    region = self.request.region
    with VMWareClient.client_session() as vmware:

        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter")

        if region.image_folder is not None:
            folder = vmware.get_folder(region.image_folder, datacenter)
            if folder is None:
                raise LookupError("Could not find VMWare VM & Templates folder.")

        datastore = vmware.get_datastore(region.image_datastore, datacenter)
        if datastore is None:
            raise LookupError("Could not find VMWare Datastore.")

    region.state = RegionState.CREATED
