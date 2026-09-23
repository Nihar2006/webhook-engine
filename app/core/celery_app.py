"""
app/core/celery_app.py
~~~~~~~~~~~~~~~~~~~~~~
Celery application singleton — Phase 3.

This module is the single source of truth for the Celery configuration.
It is imported by:
  - FastAPI routes  (to call `task.delay()`)
  - The Celery worker process  (as the ``-A`` application argument)

Usage
-----
Start the worker from the project root::

    celery -A app.core.celery_app worker --loglevel=info -P solo

The ``-P solo`` pool is recommended on Windows because Celery's default
prefork pool relies on ``os.fork()``, which is not available on that platform.

Architecture notes
------------------
* Broker  = Redis  (stores the task queue; tasks are pushed here by FastAPI)
* Backend = Redis  (stores task result metadata; optional but useful for
                   monitoring and ``AsyncResult`` lookups)
* ``autodiscover_tasks`` scans ``app.tasks`` so tasks defined there are
  registered automatically when the worker starts.
"""
from celery import Celery

from app.core.config import settings

celery_app = Celery(
    "webhook_engine",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
)

celery_app.conf.update(
    # ---- Serialisation -------------------------------------------------------
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    # ---- Time zone -----------------------------------------------------------
    timezone="UTC",
    enable_utc=True,
    # ---- Reliability ---------------------------------------------------------
    # Acknowledge the task only after the worker has *finished* executing it.
    # If the worker crashes mid-task the broker re-queues it automatically.
    task_acks_late=True,
    # Prefetch only one task at a time per worker to avoid starving the queue.
    worker_prefetch_multiplier=1,
)

# Explicitly include each task module.
#
# Why not autodiscover_tasks?
# ---------------------------
# autodiscover_tasks(["app.tasks"]) would search for a module named
# app.tasks.tasks (i.e. it appends ".tasks" to each entry).  Since our
# module is app.tasks.delivery — not app.tasks.tasks — autodiscover silently
# finds nothing and the worker starts with an empty task registry.
#
# The explicit include list is the correct, unambiguous approach whenever
# tasks live in submodules (e.g. app/tasks/delivery.py) rather than a
# conventional top-level tasks.py per Django-style app.
celery_app.conf.include = ["app.tasks.delivery"]
