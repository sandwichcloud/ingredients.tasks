import celery
from celery.utils.log import get_task_logger
from sqlalchemy.orm.exc import NoResultFound

from ingredients_db.models.images import Image
from ingredients_db.models.instance import InstanceState
from ingredients_db.models.network import Network
from ingredients_db.models.network_port import NetworkPort
from ingredients_db.models.region import Region
from ingredients_db.models.zones import Zone
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

    region: Region = self.request.session.query(Region).filter(Region.id == instance.region_id).one()
    if region.schedulable is False:
        raise LookupError("The requested region is not currently schedulable.")

    with VMWareClient.client_session() as vmware:
        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))

        vmware_image = vmware.get_image(image.file_name, datacenter)
        if vmware_image is None:
            raise LookupError("Could not find image file to clone for image %s" % str(image.id))

        old_vmware_vm = vmware.get_vm(str(instance.id), datacenter)
        if old_vmware_vm is not None:
            # A backing vm with the same id exists (how?! the task should have failed) so we probably should delete it
            logger.info(
                'A backing vm with the id of %s already exists so it is going to be deleted.' % str(instance.id))
            vmware.delete_vm(old_vmware_vm)

        network_port: NetworkPort = self.request.session.query(NetworkPort).filter(
            NetworkPort.id == instance.network_port_id).first()

        network: Network = self.request.session.query(Network).filter(
            Network.id == network_port.network_id).with_for_update().one()

        port_group = vmware.get_port_group(network.port_group, datacenter)
        if port_group is None:
            raise LookupError("Could not find port group to connect to for network %s" % str(network.id))

        dns_servers = []
        for dns_server in network.dns_servers:
            dns_servers.append(str(dns_server))

        # If multiple vms get to this point around the same time it is possible
        # that they get put into the same zone which may cause it to go over it's
        # provisioning limits.
        # Any ideas on how to fix?
        # We also need to test if a vm creating (in vmware) is in progress does it count
        # against the reported used resources
        if instance.zone_id is not None:
            zone: Zone = self.request.session.query(Zone).filter(Zone.id == instance.zone_id).one()
            if zone.schedulable is False:
                raise LookupError("The requested zone is not currently schedulable.")
                # TODO: verify zone has enough core and ram
                # if not error with "The requested zone does not have enough capacity for the instance"
        else:
            logger.info("Trying to find a suitable zone for instance %s" % str(instance.id))
            # TODO: calculate correct zone
            # we currently just return the first
            zone: Zone = self.request.session.query(Zone).filter(Zone.region_id == region.id).filter(
                Zone.schedulable == True).one()
            if zone is None:
                raise LookupError("Could not find a zone to place the instance into in the region %s" % str(region.id))

        cluster = vmware.get_cluster(zone.vm_cluster, datacenter)
        if cluster is None:
            raise LookupError("Could not find VMWare cluster for zone %s" % str(zone.id))

        datastore = vmware.get_datastore(zone.vm_datastore, datacenter)
        if datastore is None:
            raise LookupError("Could not find VMWare datastore for zone %s" % str(zone.id))

        folder = None
        if zone.vm_folder is not None:
            folder = vmware.get_folder(zone.vm_folder, datacenter)
            if folder is None:
                raise LookupError("Could not find VMWare VM & Templates folder for zone %s" % str(zone.id))

        ip_address = network_port.ip_address
        # Use the current address if port already has one
        if ip_address is None:
            logger.info('Allocating IP address for instance %s' % str(instance.id))
            # We need a nested transaction because we need to lock the network so we can calculate the next free ip
            # Without a nested transaction the lock will last for the total time of the task which could be several
            # minutes this will block the api from creating new network_ports. With nested we only block for the time
            # needed to calculate the next available ip address which is at most O(n) time with n being the number of
            # ip addresses in the cidr
            with self.database.session() as nested_session:
                nested_network_port: NetworkPort = nested_session.query(NetworkPort).filter(
                    NetworkPort.id == instance.network_port_id).first()
                # Query the network again and lock it so other things can't overlap the ip allocations
                nested_network: Network = nested_session.query(Network).filter(
                    Network.id == nested_network_port.network_id).with_for_update().one()
                ip_address = nested_network.next_free_address(nested_session)
                if ip_address is None:
                    raise IndexError("Could not allocate a free ip address. Is the pool full?")
                nested_network_port.ip_address = ip_address
                logger.info('Allocated IP address %s for instance %s' % (str(ip_address), str(instance.id)))
                nested_session.commit()

        logger.info('Creating backing vm for instance %s' % str(instance.id))
        vmware_vm = vmware.create_vm(vm_name=str(instance.id),
                                     image=vmware_image,
                                     cluster=cluster,
                                     datastore=datastore,
                                     folder=folder,
                                     port_group=port_group,
                                     ip_address=str(ip_address),
                                     gateway=str(network.gateway),
                                     subnet_mask=str(network.gateway.netmask),
                                     dns_servers=dns_servers)

        logger.info('Powering on backing vm for instance %s' % str(instance.id))
        vmware.power_on_vm(vmware_vm)

    instance.state = InstanceState.ACTIVE


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def delete_instance(self, delete_backing: bool, **kwargs):
    instance = self.request.instance
    region: Region = self.request.session.query(Region).filter(Region.id == instance.region_id).one()
    if delete_backing:
        with VMWareClient.client_session() as vmware:
            datacenter = vmware.get_datacenter(region.datacenter)
            if datacenter is None:
                raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))
            vmware_vm = vmware.get_vm(str(instance.id), datacenter)

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
    region: Region = self.request.session.query(Region).filter(Region.id == instance.region_id).one()
    with VMWareClient.client_session() as vmware:
        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))
        vmware_vm = vmware.get_vm(str(instance.id), datacenter)

        if vmware_vm is None:
            raise LookupError('Could not find backing vm for instance %s when trying to stop.' % str(instance.id))

        vmware.power_off_vm(vmware_vm, hard=hard, timeout=timeout)

    instance.state = InstanceState.STOPPED


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def start_instance(self, **kwargs):
    instance = self.request.instance
    region: Region = self.request.session.query(Region).filter(Region.id == instance.region_id).one()
    with VMWareClient.client_session() as vmware:
        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))
        vmware_vm = vmware.get_vm(str(instance.id), datacenter)

        if vmware_vm is None:
            raise LookupError('Could not find backing vm for instance %s when trying to start.' % str(instance.id))

        vmware.power_on_vm(vmware_vm)

    instance.state = InstanceState.ACTIVE


@celery.shared_task(base=InstanceTask, bind=True, max_retires=2, default_retry_delay=5)
def restart_instance(self, hard=False, timeout=60, **kwargs):
    instance = self.request.instance
    region: Region = self.request.session.query(Region).filter(Region.id == instance.region_id).one()
    with VMWareClient.client_session() as vmware:
        datacenter = vmware.get_datacenter(region.datacenter)
        if datacenter is None:
            raise LookupError("Could not find VMWare Datacenter for region %s " % str(region.id))
        vmware_vm = vmware.get_vm(str(instance.id), datacenter)

        if vmware_vm is None:
            raise LookupError('Could not find backing vm for instance %s when trying to restart.' % str(instance.id))

        vmware.power_off_vm(vmware_vm, hard=hard, timeout=timeout)
        vmware.power_on_vm(vmware_vm)

    instance.state = InstanceState.ACTIVE
