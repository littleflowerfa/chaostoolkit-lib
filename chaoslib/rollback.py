# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator

from logzero import logger

from chaoslib.activity import execute_activity
from chaoslib.types import Experiment, Run, Secrets


__all__ = ["run_rollbacks"]


def run_rollbacks(experiment: Experiment, secrets: Secrets,
                  pool: ThreadPoolExecutor,
                  dry: bool = False) -> Iterator[Run]:
    """
    Run all rollbacks declared in the experiment in their order. Wait for
    each rollback activity to complete before to the next unless the activity
    is declared with the `background` flag.
    """
    rollbacks = experiment.get("rollbacks", [])

    if not rollbacks:
        logger.info("No declared rollbacks, let's move on.")

    for activity in rollbacks:
        logger.info("Rollback: {t}".format(t=activity.get("name")))

        if activity.get("background"):
            logger.debug("rollback activity will run in the background")
            yield pool.submit(execute_activity, activity=activity,
                              secrets=secrets, dry=dry)
        else:
            yield execute_activity(activity, secrets=secrets, dry=dry)
