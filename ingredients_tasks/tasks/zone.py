import celery
from celery.utils.log import get_task_logger

from ingredients_db.models.region import Region
from ingredients_db.models.zones import ZoneState
from ingredients_tasks.tasks.tasks import ZoneTask
from ingredients_tasks.vmware import VMWareClient

logger = get_task_logger(__name__)


@celery.shared_task(base=ZoneTask, bind=True, max_retires=2, default_retry_delay=5)
def create_zone(self, **kwargs):
    zone = self.request.zone
    region: Region = self.request.session.query(Region).filter(Region.id == zone.region_id).one()
    with VMWareClient.client_session() as vmware:
        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))

        if zone.vm_folder is not None:
            folder = vmware.get_folder(zone.vm_folder, datacenter)
            if folder is None:
                raise LookupError("Could not find VMWare VM & Templates folder.")

        datastore = vmware.get_datastore(zone.vm_datastore, datacenter)
        if datastore is None:
            raise LookupError("Could not find VMWare datastore.")

        cluster = vmware.get_cluster(zone.vm_cluster, datacenter)
        if cluster is None:
            raise LookupError("Could not find VMWare cluster.")

    zone.state = ZoneState.CREATED
