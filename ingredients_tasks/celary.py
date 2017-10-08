import inspect

import celery
from kombu import Queue, Exchange
from sqlalchemy.engine.url import URL

from ingredients_db.database import Database
from ingredients_tasks.conf.loader import SETTINGS

database = Database(SETTINGS.DATABASE_HOST, SETTINGS.DATABASE_PORT, SETTINGS.DATABASE_USERNAME,
                    SETTINGS.DATABASE_PASSWORD, SETTINGS.DATABASE_DB, SETTINGS.DATABASE_POOL_SIZE)


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

        from ingredients_tasks.tasks import image, instance, network

        include, task_queues, task_routes = self.populate_tasks(image, instance, network)

        self.app.conf.update(
            broker_url=URL(**connect_args).__str__(),
            broker_transport_options={
                'confirm_publish': True
            },
            task_acks_late=True,
            task_reject_on_worker_last=True,
            task_ignore_result=True,
            task_store_errors_even_if_ignored=False,
            task_soft_time_limit=300,
            task_time_limit=600,
            worker_prefetch_multiplier=1,
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

        from ingredients_tasks.tasks.tasks import ImageTask, InstanceTask, NetworkTask

        for task_module in args:
            include.append(task_module.__name__)
            for name, method in inspect.getmembers(task_module):
                if method in [ImageTask, InstanceTask, NetworkTask]:
                    continue
                if hasattr(method, 'apply_async'):
                    task_queues.add(Queue(name, exchange=Exchange(task_module.__name__), routing_key=name))
                    task_routes[task_module.__name__ + "." + name] = {
                        'queue': name,
                        'exchange': task_module.__name__,
                        'routing_key': name
                    }

        return include, task_queues, task_routes
