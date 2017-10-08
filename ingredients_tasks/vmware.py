from contextlib import contextmanager

from pyVim import connect
from pyVmomi import vim, vmodl

from ingredients_tasks.conf.loader import SETTINGS


class VMWareClient(object):
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.service_instance = None

    def connect(self):
        self.service_instance = connect.SmartConnectNoSSL(
            host=self.host,
            user=self.username,
            pwd=self.password,
            port=self.port
        )

    def disconnect(self):
        connect.Disconnect(self.service_instance)

    def get_image(self, image_name):
        images_folder = self.get_obj([vim.Folder], SETTINGS.VCENTER_IMAGES_FOLDER)
        if images_folder is None:
            raise LookupError("Could not find Images folder with the name of %s" % SETTINGS.VCENTER_IMAGES_FOLDER)
        image = self.get_obj_in_folder([vim.VirtualMachine], images_folder, image_name)

        return image

    def delete_image(self, image):
        task = image.Destroy_Task()
        self.wait_for_tasks([task])

    def get_vm(self, vm_name):
        vms_folder = self.get_obj([vim.Folder], SETTINGS.VCENTER_INSTANCES_FOLDER)
        if vms_folder is None:
            raise LookupError("Could not find Instances folder with the name of %s" % SETTINGS.VCENTER_INSTANCES_FOLDER)
        return self.get_obj_in_folder([vim.VirtualMachine], vms_folder, vm_name)

    def create_vm(self, vm_name, image, port_group):
        vms_folder = self.get_obj([vim.Folder], SETTINGS.VCENTER_INSTANCES_FOLDER)
        if vms_folder is None:
            raise LookupError("Could not find Instances folder with the name of %s" % SETTINGS.VCENTER_INSTANCES_FOLDER)

        datastore = self.get_obj([vim.Datastore], SETTINGS.VCENTER_DATASTORE)
        if datastore is None:
            raise LookupError("Could not find datastore with the name of %s" % SETTINGS.VCENTER_DATASTORE)

        cluster = self.get_obj([vim.ClusterComputeResource], SETTINGS.VCENTER_CLUSTER)
        if cluster is None:
            raise LookupError("Could not find cluster with the name of %s" % SETTINGS.VCENTER_CLUSTER)

        relospec = vim.vm.RelocateSpec()
        relospec.datastore = datastore
        relospec.pool = cluster.resourcePool
        # relospec.pool = resource_pool  # Do we want to support resource pools? We need a better environment to test

        clonespec = vim.vm.CloneSpec()
        clonespec.location = relospec
        clonespec.powerOn = False

        dvs_port_connection = vim.dvs.PortConnection()
        dvs_port_connection.portgroupKey = port_group.key
        dvs_port_connection.switchUuid = (
            port_group.config.distributedVirtualSwitch.uuid
        )

        nic = vim.vm.device.VirtualDeviceSpec()
        nic.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
        nic.device = vim.vm.device.VirtualVmxnet3()
        nic.device.addressType = 'assigned'
        nic.device.key = 4000
        nic.device.backing = vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo()
        nic.device.backing.port = dvs_port_connection
        nic.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
        nic.device.connectable.startConnected = True
        nic.device.connectable.allowGuestControl = True

        vmconf = vim.vm.ConfigSpec()
        vmconf.numCPUs = 1
        vmconf.memoryMB = 1024
        vmconf.deviceChange = [nic]
        vmconf.bootOptions = vim.vm.BootOptions()

        enable_uuid_opt = vim.option.OptionValue()
        enable_uuid_opt.key = 'disk.enableUUID'
        enable_uuid_opt.value = '1'
        vmconf.extraConfig = [enable_uuid_opt]

        bootDiskDevice = vim.vm.BootOptions.BootableDiskDevice()
        bootDiskDevice.deviceKey = 2000

        vmconf.bootOptions.bootOrder = [bootDiskDevice]

        clonespec.config = vmconf

        # customspec = vim.vm.customization.Specification()
        #
        # guest_map = vim.vm.customization.AdapterMapping()
        # guest_map.adapter = vim.vm.customization.IPSettings()
        # guest_map.adapter.ip = vim.vm.customization.FixedIp()
        # guest_map.adapter.ip.ipAddress = '10.20.0.15'
        # guest_map.adapter.subnetMask = '255.255.255.0'
        # guest_map.adapter.gateway = '10.20.0.1'
        #
        # globalip = vim.vm.customization.GlobalIPSettings()
        # globalip.dnsServerList = ['8.8.8.8', '8.8.4.4']
        # globalip.dnsSuffixList = 'rmb938.me'
        # customspec.globalIPSettings = globalip
        #
        # customspec.nicSettingMap = [guest_map]
        #
        # ident = vim.vm.customization.LinuxPrep()
        # ident.domain = 'rmb938.me'
        # ident.hostName = vim.vm.customization.FixedName()
        # ident.hostName.name = 'woovm'
        #
        # customspec.identity = ident
        # clonespec.customization = customspec

        # TODO: guest customizations with dns, gateway and ip

        task = image.Clone(folder=vms_folder, name=vm_name, spec=clonespec)
        self.wait_for_tasks([task])

        return self.get_vm(vm_name)

    def power_on_vm(self, vm):
        task = vm.PowerOn()
        self.wait_for_tasks([task])

    def delete_vm(self, vm):
        task = vm.Destroy_task()
        self.wait_for_tasks([task])

    def find_vm_mac(self, vm):
        for device in vm.config.hardware.device:
            if isinstance(device, vim.vm.device.VirtualEthernetCard):
                return device.macAddress

        return None

    def get_port_group(self, port_group_name):
        port_group = self.get_obj([vim.dvs.DistributedVirtualPortgroup], port_group_name)

        return port_group

    @classmethod
    @contextmanager
    def client_session(cls):
        vmware_client = VMWareClient(SETTINGS.VCENTER_HOST, SETTINGS.VCENTER_PORT, SETTINGS.VCENTER_USERNAME,
                                     SETTINGS.VCENTER_PASSWORD)
        vmware_client.connect()

        try:
            yield vmware_client
        finally:
            vmware_client.disconnect()

    def get_obj(self, vimtype, name):
        """
        Return an object by name, if name is None the
        first found object is returned
        """
        obj = None
        content = self.service_instance.RetrieveContent()
        container = content.viewManager.CreateContainerView(content.rootFolder, vimtype, True)
        for c in container.view:
            if name:
                if c.name == name:
                    obj = c
                    break
            else:
                obj = c
                break

        container.Destroy()
        return obj

    def get_obj_in_folder(self, vimtype, folder, name):
        obj = None
        content = self.service_instance.RetrieveContent()
        container = content.viewManager.CreateContainerView(folder, vimtype, True)
        for c in container.view:
            if name:
                if c.name == name:
                    obj = c
                    break
            else:
                obj = c
                break

        return obj

    def wait_for_tasks(self, tasks):
        """Given the service instance si and tasks, it returns after all the
       tasks are complete
       """
        property_collector = self.service_instance.RetrieveContent().propertyCollector
        task_list = [str(task) for task in tasks]
        # Create filter
        obj_specs = [vmodl.query.PropertyCollector.ObjectSpec(obj=task)
                     for task in tasks]
        property_spec = vmodl.query.PropertyCollector.PropertySpec(type=vim.Task,
                                                                   pathSet=[],
                                                                   all=True)
        filter_spec = vmodl.query.PropertyCollector.FilterSpec()
        filter_spec.objectSet = obj_specs
        filter_spec.propSet = [property_spec]
        pcfilter = property_collector.CreateFilter(filter_spec, True)
        try:
            version, state = None, None
            # Loop looking for updates till the state moves to a completed state.
            while len(task_list):
                update = property_collector.WaitForUpdates(version)
                for filter_set in update.filterSet:
                    for obj_set in filter_set.objectSet:
                        task = obj_set.obj
                        for change in obj_set.changeSet:
                            if change.name == 'info':
                                state = change.val.state
                            elif change.name == 'info.state':
                                state = change.val
                            else:
                                continue

                            if not str(task) in task_list:
                                continue

                            if state == vim.TaskInfo.State.success:
                                # Remove task from taskList
                                task_list.remove(str(task))
                            elif state == vim.TaskInfo.State.error:
                                raise task.info.error
                # Move to next version
                version = update.version
        finally:
            if pcfilter:
                pcfilter.Destroy()
