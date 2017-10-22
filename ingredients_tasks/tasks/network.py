import celery

from ingredients_db.models.network import NetworkState
from ingredients_tasks.tasks.tasks import NetworkTask
from ingredients_tasks.vmware import VMWareClient


@celery.shared_task(base=NetworkTask, bind=True, max_retires=2, default_retry_delay=5)
def create_network(self, **kwargs):
    network = self.request.network
    with VMWareClient.client_session() as vmware:
        port_group = vmware.get_port_group(network.port_group)

        if port_group is None:
            raise ValueError("Could not find port group")

    network.state = NetworkState.CREATED
