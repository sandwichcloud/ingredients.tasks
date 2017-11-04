import celery
from celery.utils.log import get_task_logger
from sqlalchemy.orm.exc import NoResultFound

from ingredients_db.models.images import Image
from ingredients_db.models.instance import InstanceState
from ingredients_db.models.network import Network
from ingredients_db.models.network_port import NetworkPort
from ingredients_tasks.tasks.tasks import InstanceTask
from ingredients_tasks.vmware import VMWareClient

logger = get_task_logger(__name__)


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def create_instance(self, **kwargs):
    instance = self.request.instance
    if instance.image_id is None:
        raise ValueError("Image turned NULL before the instance could be created")

    try:
        image = self.request.session.query(Image).filter(Image.id == instance.image_id).one()
    except NoResultFound:
        raise LookupError("Image got deleted before the instance could be created")

    with VMWareClient.client_session() as vmware:
        vmware_image = vmware.get_image(image.file_name)

        if vmware_image is None:
            raise LookupError("Could not find image file to clone")

        old_vmware_vm = vmware.get_vm(str(instance.id))
        if old_vmware_vm is not None:
            # A backing vm with the same id exists (how?! the task should have failed) so we probably should delete it
            logger.info(
                'A backing vm with the id of %s already exists so it is going to be deleted.' % str(instance.id))
            vmware.delete_vm(old_vmware_vm)

        # We need a nested transaction because we need to lock the network so we can calculate the next free ip
        # Without a nested transaction the lock will last for the total time of the task which could be several minutes
        # this will block the api from creating new network_ports. With nested we only block for the time needed to
        # calculate the next available ip address which is at most O(n) time with n being the number of
        # ip addresses in the cidr
        with self.database.session() as nested_session:
            network_port: NetworkPort = nested_session.query(NetworkPort).filter(
                NetworkPort.id == instance.network_port_id).first()

            network: Network = nested_session.query(Network).filter(
                Network.id == network_port.network_id).with_for_update().first()

            dns_servers = []
            for dns_server in network.dns_servers:
                dns_servers.append(str(dns_server))

            logger.info('Allocating IP address for instance %s' % str(instance.id))
            if network_port.ip_address is not None:
                # An ip address was already allocated (how?! the task should have failed) so let's reset it
                network_port.ip_address = None

            ip_address = network.next_free_address(nested_session)
            if ip_address is None:
                raise IndexError("Could not allocate a free ip address. Is the pool full?")
            network_port.ip_address = ip_address
            logger.info('Allocated IP address %s for instance %s' % (str(ip_address), str(instance.id)))

            port_group = vmware.get_port_group(network.port_group)
            if port_group is None:
                raise LookupError("Cloud not find port group to connect to")
            nested_session.commit()

        logger.info('Creating backing vm for instance %s' % str(instance.id))
        vmware_vm = vmware.create_vm(vm_name=str(instance.id), image=vmware_image, port_group=port_group,
                                     ip_address=str(ip_address), gateway=str(network.gateway),
                                     subnet_mask=str(network.gateway.netmask), dns_servers=dns_servers)

        logger.info('Powering on backing vm for instance %s' % str(instance.id))
        vmware.power_on_vm(vmware_vm)

    instance.state = InstanceState.ACTIVE


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def delete_instance(self, delete_backing: bool, **kwargs):
    instance = self.request.instance
    if delete_backing:
        with VMWareClient.client_session() as vmware:
            vmware_vm = vmware.get_vm(str(instance.id))

            if vmware_vm is None:
                logger.warning(
                    'Could not find backing vm for instance %s when trying to delete.' % str(instance.id))
            else:
                logger.info('Deleting backing vm for instance %s' % str(instance.id))
                vmware.power_off_vm(vmware_vm)
                vmware.delete_vm(vmware_vm)

    network_port = self.request.session.query(NetworkPort).filter(NetworkPort.id == instance.network_port_id).first()

    instance.state = InstanceState.DELETED
    self.request.session.delete(instance)
    self.request.session.flush()
    self.request.session.delete(network_port)


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def stop_instance(self, hard=False, timeout=60, **kwargs):
    instance = self.request.instance
    with VMWareClient.client_session() as vmware:
        vmware_vm = vmware.get_vm(str(instance.id))

        if vmware_vm is None:
            raise LookupError('Could not find backing vm for instance %s when trying to stop.' % str(instance.id))

        vmware.power_off_vm(vmware_vm, hard=hard, timeout=timeout)

    instance.state = InstanceState.STOPPED


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def start_instance(self, **kwargs):
    instance = self.request.instance
    with VMWareClient.client_session() as vmware:
        vmware_vm = vmware.get_vm(str(instance.id))

        if vmware_vm is None:
            raise LookupError('Could not find backing vm for instance %s when trying to start.' % str(instance.id))

        vmware.power_on_vm(vmware_vm)

    instance.state = InstanceState.ACTIVE


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def restart_instance(self, hard=False, timeout=60, **kwargs):
    instance = self.request.instance
    with VMWareClient.client_session() as vmware:
        vmware_vm = vmware.get_vm(str(instance.id))

        if vmware_vm is None:
            raise LookupError('Could not find backing vm for instance %s when trying to restart.' % str(instance.id))

        vmware.power_off_vm(vmware_vm, hard=hard, timeout=timeout)
        vmware.power_on_vm(vmware_vm)

    instance.state = InstanceState.ACTIVE
