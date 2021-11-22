from __future__ import absolute_import

from celery import Celery, current_task
from celery.exceptions import  Reject

import os


#import logging
#logger = logging.getLogger(__name__)

from celery import shared_task

celery_app = Celery('tasks', broker='redis://localhost:6379/1', backend='redis://localhost:6379/1')
#celery_app.autodiscover_tasks()

@celery_app.task(name="add")
def add(x, y):
    # Only for testing...
    return x + y
