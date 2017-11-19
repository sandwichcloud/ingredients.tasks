import inspect

import celery
from kombu import Queue, Exchange
from sqlalchemy.engine.url import URL


class Messaging(object):
    def __init__(self, host, port, username, password, vhost):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.vhost = vhost
        self.app = celery.Celery()

    def connect(self):
        connect_args = {
            'drivername': 'amqp',
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'password': self.password,
            'database': self.vhost
        }

        from ingredients_tasks.tasks import image, instance, network, region, zone

        include, task_queues, task_routes = self.populate_tasks(image, instance, network, region, zone)

        self.app.conf.update(
            broker_url=URL(**connect_args).__str__(),
            broker_transport_options={
                'confirm_publish': True
            },
            task_acks_late=True,
            task_reject_on_worker_lost=True,
            task_ignore_result=True,
            task_store_errors_even_if_ignored=False,
            task_soft_time_limit=300,  # 5 minutes
            task_time_limit=600,  # 10 minutes
            worker_prefetch_multiplier=1,  # One worker process can only do one type of task at a time
            include=include,
            task_queues=task_queues,
            task_routes=task_routes
        )

    def start(self, argv):
        self.app.start(argv=argv)

    def populate_tasks(self, *args):
        include = []
        task_queues = set()
        task_routes = {}

        from ingredients_tasks.tasks.tasks import NetworkTask, ImageTask, InstanceTask, RegionTask, ZoneTask

        for task_module in args:
            include.append(task_module.__name__)
            for name, method in inspect.getmembers(task_module):
                if method in [NetworkTask, ImageTask, InstanceTask, RegionTask, ZoneTask]:
                    continue
                if hasattr(method, 'apply_async'):
                    task_queues.add(Queue(name, exchange=Exchange(task_module.__name__), routing_key=name))
                    task_routes[task_module.__name__ + "." + name] = {
                        'queue': name,
                        'exchange': task_module.__name__,
                        'routing_key': name
                    }

        return include, task_queues, task_routes
