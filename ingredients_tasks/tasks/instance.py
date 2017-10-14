import celery
from celery.utils.log import get_task_logger
from sqlalchemy.orm.exc import NoResultFound

from ingredients_db.models.images import Image
from ingredients_db.models.instance import InstanceState
from ingredients_db.models.network import Network
from ingredients_db.models.network_port import NetworkPort
from ingredients_tasks.tasks.tasks import InstanceTask

logger = get_task_logger(__name__)


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def create_instance(self, **kwargs):
    if self.instance.image_id is None:
        raise ValueError("Image turned NULL before the instance could be created")

    try:
        image = self.db_session.query(Image).filter(Image.id == self.instance.image_id).one()
    except NoResultFound:
        raise LookupError("Image got deleted before the instance could be created")

    vmware_image = self.vmware_session.get_image(image.file_name)

    if vmware_image is None:
        raise LookupError("Could not find image file to clone")

    old_vmware_vm = self.vmware_session.get_vm(str(self.instance.id))
    if old_vmware_vm is not None:
        # A backing vm with the same id exists (how?!) so we probably should delete it
        logger.info(
            'A backing vm with the id of %s already exists so it is going to be deleted.' % str(self.instance.id))
        self.vmware_session.delete_vm(old_vmware_vm)

    network_port = self.db_session.query(NetworkPort).filter(
        NetworkPort.id == self.instance.network_port_id).first()

    network = self.db_session.query(Network).filter(Network.id == network_port.network_id).first()

    logger.info('Allocating IP address for instance %s' % str(self.instance.id))
    ip_address = network.next_free_address(self.db_session)
    if ip_address is None:
        raise IndexError("Could not allocate a free ip address. Is the pool full?")
    network_port.ip_address = ip_address

    port_group = self.vmware_session.get_port_group(network.port_group)
    if port_group is None:
        raise LookupError("Cloud not find port group to connect to")

    logger.info('Creating backing vm for instance %s' % str(self.instance.id))
    vmware_vm = self.vmware_session.create_vm(vm_name=str(self.instance.id), image=vmware_image, port_group=port_group)

    nic_mac = self.vmware_session.find_vm_mac(vmware_vm)
    if nic_mac is None:
        raise LookupError("Could not find mac address of nic")

    logger.info('Telling DHCP about our IP for instance %s' % str(self.instance.id))
    self.omapi_session.add_host(str(network_port.ip_address), nic_mac)

    logger.info('Powering on backing vm for instance %s' % str(self.instance.id))
    self.vmware_session.power_on_vm(vmware_vm)

    self.instance.state = InstanceState.ACTIVE


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def delete_instance(self, delete_backing: bool, **kwargs):
    if delete_backing:
        vmware_vm = self.vmware_session.get_vm(str(self.instance.id))

        if vmware_vm is None:
            logger.warning('Could not find backing vm for instance %s when trying to delete.' % str(self.instance.id))
        else:
            logger.info('Deleting backing vm for instance %s' % str(self.instance.id))
            self.vmware_session.power_off_vm(vmware_vm)
            self.vmware_session.delete_vm(vmware_vm)

    network_port = self.db_session.query(NetworkPort).filter(
        NetworkPort.id == self.instance.network_port_id).first()

    self.instance.state = InstanceState.DELETED
    self.db_session.delete(self.instance)
    self.db_session.delete(network_port)


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def stop_instance(self, hard=False, timeout=60, **kwargs):
    vmware_vm = self.vmware_session.get_vm(str(self.instance.id))

    if vmware_vm is None:
        raise LookupError('Could not find backing vm for instance %s when trying to stop.' % str(self.instance.id))

    self.vmware_session.power_off_vm(vmware_vm, hard=hard, timeout=timeout)

    self.instance.state = InstanceState.STOPPED


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def start_instance(self, **kwargs):
    vmware_vm = self.vmware_session.get_vm(str(self.instance.id))

    if vmware_vm is None:
        raise LookupError('Could not find backing vm for instance %s when trying to start.' % str(self.instance.id))

    self.vmware_session.power_on_vm(vmware_vm)

    self.instance.state = InstanceState.ACTIVE


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def restart_instance(self, hard=False, timeout=60, **kwargs):
    vmware_vm = self.vmware_session.get_vm(str(self.instance.id))

    if vmware_vm is None:
        raise LookupError('Could not find backing vm for instance %s when trying to restart.' % str(self.instance.id))

    self.vmware_session.power_off_vm(vmware_vm, hard=hard, timeout=timeout)
    self.vmware_session.power_on_vm(vmware_vm)

    self.instance.state = InstanceState.ACTIVE
