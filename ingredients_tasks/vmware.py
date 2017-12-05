import time
from contextlib import contextmanager

from pyVim import connect
from pyVmomi import vim, vmodl
from simple_settings import settings


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

    def get_datacenter(self, datacenter_name):
        return self.get_obj(vim.Datacenter, datacenter_name)

    def get_folder(self, folder_name, datacenter):
        # TODO: find folder in DC
        return self.get_obj(vim.Folder, folder_name)

    def get_image(self, image_name, datacenter):
        return self.get_obj(vim.VirtualMachine, image_name, folder=datacenter.vmFolder)

    def get_vm(self, vm_name, datacenter):
        return self.get_obj(vim.VirtualMachine, vm_name, folder=datacenter.vmFolder)

    def get_cluster(self, cluster_name, datacenter):
        return self.get_obj(vim.ClusterComputeResource, cluster_name, folder=datacenter.hostFolder)

    def get_datastore(self, datastore_name, datacenter):
        return self.get_obj(vim.Datastore, datastore_name, folder=datacenter.datastoreFolder)

    def get_port_group(self, port_group_name, datacenter):
        return self.get_obj(vim.dvs.DistributedVirtualPortgroup, port_group_name, folder=datacenter.networkFolder)

    def create_vm(self, vm_name, image, datacenter, cluster, datastore, folder, port_group, ip_address, gateway,
                  subnet_mask, dns_servers):

        relospec = vim.vm.RelocateSpec()
        relospec.datastore = datastore
        relospec.pool = cluster.resourcePool

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

        globalip = vim.vm.customization.GlobalIPSettings()
        globalip.dnsServerList = dns_servers

        guest_map = vim.vm.customization.AdapterMapping()
        guest_map.adapter = vim.vm.customization.IPSettings()
        guest_map.adapter.ip = vim.vm.customization.FixedIp()
        guest_map.adapter.ip.ipAddress = ip_address
        guest_map.adapter.subnetMask = subnet_mask
        guest_map.adapter.gateway = gateway

        ident = vim.vm.customization.LinuxPrep()
        ident.domain = 'sandwich.local'
        ident.hostName = vim.vm.customization.FixedName()
        ident.hostName.name = 'ip-' + ip_address.replace(".", "-")

        customspec = vim.vm.customization.Specification()
        customspec.nicSettingMap = [guest_map]
        customspec.globalIPSettings = globalip
        customspec.identity = ident

        clonespec.customization = customspec

        vmconf = vim.vm.ConfigSpec()
        vmconf.numCPUs = 1  # TODO: allow customization of these
        vmconf.memoryMB = 1024
        vmconf.deviceChange = [nic]

        vmconf.bootOptions = vim.vm.BootOptions()
        # Set the boot device to the first disk just in-case it was set to something else
        boot_disk_device = vim.vm.BootOptions.BootableDiskDevice()
        boot_disk_device.deviceKey = 2000
        vmconf.bootOptions.bootOrder = [boot_disk_device]

        enable_uuid_opt = vim.option.OptionValue()
        enable_uuid_opt.key = 'disk.enableUUID'  # Allow the guest to easily mount extra disks
        enable_uuid_opt.value = '1'
        vmconf.extraConfig = [enable_uuid_opt]

        clonespec.config = vmconf

        if folder is not None:
            task = image.Clone(folder=folder, name=vm_name, spec=clonespec)
        else:
            task = image.Clone(name=vm_name, spec=clonespec)

        self.wait_for_tasks([task])

        vm = self.get_vm(vm_name, datacenter)

        # TODO: uncomment when we can specify disk size
        # # Resize Storage Drive
        # virtual_disk_device = None
        #
        # # Find the disk device
        # for dev in vm.config.hardware.device:
        #     if isinstance(dev, vim.vm.device.VirtualDisk) and dev.deviceInfo.label == "Hard disk 1":
        #         virtual_disk_device = dev
        #         break
        #
        # virtual_disk_spec = vim.vm.device.VirtualDeviceSpec()
        # virtual_disk_spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
        # virtual_disk_spec.device = virtual_disk_device
        # virtual_disk_spec.device.capacityInBytes = 100 * (1024 ** 3) # 100GB disk
        #
        # spec = vim.vm.ConfigSpec()
        # spec.deviceChange = [virtual_disk_spec]
        # task = vm.ReconfigVM_Task(spec=spec)
        # self.wait_for_tasks([task])

        return vm

    def power_on_vm(self, vm):
        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff:
            task = vm.PowerOn()
            self.wait_for_tasks([task])

    def power_off_vm(self, vm, hard=False, timeout=60):
        if vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
            if hard is False:
                try:
                    vm.ShutdownGuest()
                except vim.fault.ToolsUnavailable:
                    # Guest tools was not running to hard power off instead
                    return self.power_off_vm(vm, hard=True)
                # Poll every 5 seconds until powered off or timeout
                while vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOn:
                    if timeout <= 0:
                        break
                    timeout -= 5
                    time.sleep(5)
                else:
                    # VM has finished powering off
                    return

            task = vm.PowerOff()
            self.wait_for_tasks([task])

    def delete_vm(self, vm):
        task = vm.Destroy()
        self.wait_for_tasks([task])

    def delete_image(self, image):
        task = image.Destroy_Task()
        self.wait_for_tasks([task])

    def template_vm(self, vm, datastore, folder):
        reloSpec = vim.vm.RelocateSpec()
        reloSpec.datastore = datastore
        # the vm template stays on the host
        # if the host is down any clone will fail

        if folder is not None:
            reloSpec.folder = folder

        task = vm.RelocateVM_Task(reloSpec)
        self.wait_for_tasks([task])
        vm.MarkAsTemplate()  # This doesn't return a task?

    def get_obj(self, vimtype, name, folder=None):
        """
        Return an object by name, if name is None the
        first found object is returned
        """
        obj = None
        content = self.service_instance.RetrieveContent()

        if folder is None:
            folder = content.rootFolder

        container = content.viewManager.CreateContainerView(folder, [vimtype], True)
        for c in container.view:
            if c.name == name:
                obj = c
                break

        container.Destroy()
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

    @classmethod
    @contextmanager
    def client_session(cls):
        vmware_client = VMWareClient(settings.VCENTER_HOST, settings.VCENTER_PORT, settings.VCENTER_USERNAME,
                                     settings.VCENTER_PASSWORD)
        vmware_client.connect()

        try:
            yield vmware_client
        finally:
            vmware_client.disconnect()
