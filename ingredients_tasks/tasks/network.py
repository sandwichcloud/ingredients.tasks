import celery

from ingredients_db.models.network import NetworkState
from ingredients_tasks.tasks.tasks import NetworkTask


@celery.shared_task(base=NetworkTask, bind=True, max_retires=2, default_retry_delay=5)
def create_network(self, **kwargs):
    port_group = self.vmware_session.get_port_group(self.network.port_group)

    if port_group is None:
        raise ValueError("Could not find port group")

    self.network.state = NetworkState.CREATED
