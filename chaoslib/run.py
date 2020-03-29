# -*- coding: utf-8 -*-
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
import platform
import threading
import time
from typing import Any, Dict, List, NoReturn, Optional

from logzero import logger

from chaoslib import __version__
from chaoslib.activity import run_activities
from chaoslib.control import initialize_controls, controls, cleanup_controls, \
    Control, initialize_global_controls, cleanup_global_controls
from chaoslib.exceptions import ChaosException, InterruptExecution
from chaoslib.configuration import load_configuration
from chaoslib.hypothesis import run_steady_state_hypothesis
from chaoslib.rollback import run_rollbacks
from chaoslib.secret import load_secrets
from chaoslib.settings import get_loaded_settings
from chaoslib.types import Configuration, Experiment, Journal, Run, Secrets, \
    Settings, Schedule, Strategy

__all__ = ["Runner", "RunEventHandler"]


class RunEventHandler:
    """
    Base class to react to certain, or all, events during an execution.

    This is mainly meant for reacting the execution's mainloop. Do not
    implement it as part of an extension, use the Control interface instead.
    """
    def started(self, experiment: Experiment, journal: Journal) -> NoReturn:
        pass

    def finish(self, journal: Journal) -> NoReturn:
        pass

    def interrupted(self, experiment: Experiment,
                    journal: Journal) -> NoReturn:
        pass

    def signal_exit(self) -> NoReturn:
        pass

    def start_continous_hypothesis(self, frequency: int) -> NoReturn:
        pass

    def continous_hypothesis_iteration(self, iteration_index: int,
                                       state: Any) -> NoReturn:
        pass

    def continous_hypothesis_completed(self) -> NoReturn:
        pass

    def start_method(self, iteration_index: int = 0) -> NoReturn:
        pass

    def condition_completed(self, state: Any,
                            iteration_index: int = 0) -> NoReturn:
        pass

    def start_cooldown(self, duration: int) -> NoReturn:
        pass

    def cooldown_completed(self) -> NoReturn:
        pass


class EventHandlerRegistry:
    def __init__(self):
        self.handlers = []

    def register(self, handler: RunEventHandler) -> NoReturn:
        self.handlers.append(handler)

    def started(self, experiment: Experiment, journal: Journal) -> NoReturn:
        for h in self.handlers:
            try:
                h.started(experiment, journal)
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def finish(self, journal: Journal) -> NoReturn:
        for h in self.handlers:
            try:
                h.finish(journal)
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def interrupted(self, experiment: Experiment,
                    journal: Journal) -> NoReturn:
        for h in self.handlers:
            try:
                h.interrupted(experiment, journal)
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def signal_exit(self) -> NoReturn:
        for h in self.handlers:
            try:
                h.signal_exit()
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def start_continous_hypothesis(self, frequency: int) -> NoReturn:
        for h in self.handlers:
            try:
                h.start_continous_hypothesis(frequency)
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def continous_hypothesis_iteration(self, iteration_index: int,
                                       state: Any) -> NoReturn:
        for h in self.handlers:
            try:
                h.continous_hypothesis_iteration(iteration_index, state)
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def continous_hypothesis_completed(self) -> NoReturn:
        for h in self.handlers:
            try:
                h.continous_hypothesis_completed()
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def start_method(self, iteration_index: int = 0) -> NoReturn:
        for h in self.handlers:
            try:
                h.start_method(iteration_index)
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def condition_completed(self, state: Any,
                            iteration_index: int = 0) -> NoReturn:
        for h in self.handlers:
            try:
                h.condition_completed(state, iteration_index)
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def start_cooldown(self, duration: int) -> NoReturn:
        for h in self.handlers:
            try:
                h.start_cooldown(duration)
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)

    def cooldown_completed(self) -> NoReturn:
        for h in self.handlers:
            try:
                h.cooldown_completed()
            except Exception:
                logger.debug(
                    "Handler {} failed".format(
                        h.__class__.__name__), exc_info=True)


class Runner:
    def __init__(self, strategy: Strategy, schedule: Schedule = None):
        self.strategy = strategy
        self.schedule = schedule or Schedule()
        self.event_registry = EventHandlerRegistry()

    def __enter__(self) -> 'Runner':
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, tb: Any) -> NoReturn:
        self.cleanup()

    def register_event_handler(self, handler: RunEventHandler) -> NoReturn:
        self.event_registry.register(handler)

    def configure(self, experiment: Experiment, settings: Settings,
                  journal: Journal) -> NoReturn:
        self.settings = settings if settings is not None else \
            get_loaded_settings()
        self.config = load_configuration(experiment.get("configuration", {}))
        self.secrets = load_secrets(experiment.get("secrets", {}), self.config)
        self.journal = journal or initialize_run_journal(experiment)

    def cleanup(self):
        pass

    def run(self, experiment: Experiment,
            settings: Settings = None, journal: Journal = None) -> Journal:

        self.configure(experiment, settings, journal)
        self._run(
            self.strategy, self.schedule, experiment, self.journal,
            self.config, self.secrets, self.settings, self.event_registry)
        return self.journal

    def _run(self, strategy: Strategy, schedule: Schedule,  # noqa: C901
             experiment: Experiment, journal: Journal,
             configuration: Configuration, secrets: Secrets,
             settings: Settings,
             event_registry: EventHandlerRegistry) -> NoReturn:
        logger.info("Running experiment: {t}".format(t=experiment["title"]))
        event_registry.started(experiment, journal)

        started_at = time.time()
        control = Control()
        activity_pool, rollback_pool = get_background_pools(experiment)
        hypo_pool = get_hypothesis_pool()
        continous_hypo_event = threading.Event()

        dry = experiment.get("dry", False)
        if dry:
            logger.warning("Dry mode enabled")

        initialize_global_controls(
            experiment, configuration, secrets, settings)
        initialize_controls(experiment, configuration, secrets)

        try:
            try:
                control.begin(
                    "experiment", experiment, experiment, configuration,
                    secrets)

                state = object()
                if should_run_before_method(strategy):
                    state = run_gate_hypothesis(
                        experiment, journal, configuration, secrets, dry)

                if state is not None:
                    if should_run_during_method(strategy):
                        run_hypothesis_during_method(
                            hypo_pool, continous_hypo_event, strategy,
                            schedule, experiment, journal, configuration,
                            secrets, event_registry, dry)

                    state = run_method(
                         strategy, activity_pool, experiment, journal,
                         configuration, secrets, event_registry, dry)

                    continous_hypo_event.set()
                    if journal["status"] not in ["interrupted", "aborted"]:
                        if state is not None and \
                                should_run_after_method(strategy):
                            run_deviation_validation_hypothesis(
                                experiment, journal, configuration, secrets,
                                dry)
            except InterruptExecution as i:
                journal["status"] = "interrupted"
                logger.fatal(str(i))
                event_registry.interrupted(experiment, journal)
            except (KeyboardInterrupt, SystemExit):
                journal["status"] = "interrupted"
                logger.warning("Received an exit signal, "
                               "leaving without applying rollbacks.")
                event_registry.signal_exit()
            else:
                hypo_pool.shutdown(wait=True)
                journal["status"] = journal["status"] or "completed"
                try:
                    journal["rollbacks"] = apply_rollbacks(
                        experiment, configuration, secrets, rollback_pool, dry)
                except InterruptExecution as i:
                    journal["status"] = "interrupted"
                    logger.fatal(str(i))
                except (KeyboardInterrupt, SystemExit):
                    journal["status"] = "interrupted"
                    logger.warning(
                        "Received an exit signal."
                        "Terminating now without running the "
                        "remaining rollbacks.")

            hypo_pool.shutdown(wait=True)
            journal["end"] = datetime.utcnow().isoformat()
            journal["duration"] = time.time() - started_at
            has_deviated = journal["deviated"]
            status = "deviated" if has_deviated else journal["status"]
            logger.info("Experiment ended with status: {s}".format(s=status))
            if has_deviated:
                logger.info(
                    "The steady-state has deviated, a weakness may have been "
                    "discovered")

            control.with_state(self.journal)
            try:
                control.end(
                    "experiment", experiment, experiment, configuration,
                    secrets)
            except ChaosException:
                logger.debug("Failed to close controls", exc_info=True)
        finally:
            try:
                cleanup_controls(experiment)
                cleanup_global_controls()
            finally:
                event_registry.finish(journal)


def should_run_before_method(strategy: Strategy) -> bool:
    return strategy in [
        Strategy.BEFORE_METHOD, Strategy.DEFAULT, Strategy.CONTINOUS]


def should_run_after_method(strategy: Strategy) -> bool:
    return strategy in [
        Strategy.AFTER_METHOD, Strategy.DEFAULT, Strategy.CONTINOUS]


def should_run_during_method(strategy: Strategy) -> bool:
    return strategy in [Strategy.DURING_METHOD, Strategy.CONTINOUS]


def run_gate_hypothesis(experiment: Experiment, journal: Journal,
                        configuration: Configuration, secrets: Secrets,
                        dry: bool = False) -> Dict[str, Any]:
    """
    Run the hypothesis before the method and bail the execution if it did
    not pass.
    """
    logger.debug("Running steady-state hypothesis before the method")
    state = run_steady_state_hypothesis(
        experiment, configuration, secrets, dry=dry)
    journal["steady_states"]["before"] = state
    if state is not None and not state["steady_state_met"]:
        journal["steady_states"]["before"] = state
        journal["status"] = "failed"

        p = state["probes"][-1]
        logger.fatal(
            "Steady state probe '{p}' is not in the given "
            "tolerance so failing this experiment".format(
                p=p["activity"]["name"]))
    return state


def run_deviation_validation_hypothesis(experiment: Experiment,
                                        journal: Journal,
                                        configuration: Configuration,
                                        secrets: Secrets, dry: bool = False) \
                                            -> Dict[str, Any]:
    """
    Run the hypothesis after the method and report to the journal if the
    experiment has deviated.
    """
    logger.debug("Running steady-state hypothesis after the method")
    state = run_steady_state_hypothesis(
        experiment, configuration, secrets, dry=dry)
    journal["steady_states"]["after"] = state
    if state is not None and \
            not state["steady_state_met"]:
        journal["deviated"] = True
        journal["status"] = "failed"
        p = state["probes"][-1]
        logger.fatal(
            "Steady state probe '{p}' is not in the "
            "given tolerance so failing this "
            "experiment".format(
                p=p["activity"]["name"]))
    return state


def run_hypothesis_during_method(hypo_pool: ThreadPoolExecutor,
                                 continous_hypo_event: threading.Event,
                                 strategy: Strategy, schedule: Schedule,
                                 experiment: Experiment, journal: Journal,
                                 configuration: Configuration,
                                 secrets: Secrets,
                                 event_registry: EventHandlerRegistry,
                                 dry: bool = False) -> Future:
    """
    Run the hypothesis continously in a background thead and report the
    status in the journal when it raised an exception.
    """
    def completed(f: Future):
        exc = f.exception()
        event_registry.continous_hypothesis_completed()
        if exc is not None:
            if isinstance(exc, InterruptExecution):
                journal["status"] = "interrupted"
                logger.fatal(str(exc))
            elif isinstance(exc, Exception):
                journal["status"] = "aborted"
                logger.fatal(str(exc))
        logger.info("Continous steady state hypothesis terminated")

    f = hypo_pool.submit(
        run_hypothesis_continuously, continous_hypo_event,
        schedule, experiment, journal, configuration, secrets,
        event_registry, dry=dry)
    f.add_done_callback(completed)
    return f


def run_method(strategy: Strategy, activity_pool: ThreadPoolExecutor,
               experiment: Experiment, journal: Journal,
               configuration: Configuration, secrets: Secrets,
               event_registry: EventHandlerRegistry,
               dry: bool = False) -> Optional[List[Run]]:
    event_registry.start_method()
    try:
        state = apply_activities(
            experiment, configuration, secrets, activity_pool, journal, dry)
        journal["run"] = state
        event_registry.condition_completed(state)
        return journal["run"]
    except InterruptExecution:
        event_registry.condition_completed(None)
        raise
    except Exception:
        journal["status"] = "aborted"
        event_registry.condition_completed(None)
        logger.fatal(
            "Experiment ran into an un expected fatal error, "
            "aborting now.", exc_info=True)


def initialize_run_journal(experiment: Experiment) -> Journal:
    return {
        "chaoslib-version": __version__,
        "platform": platform.platform(),
        "node": platform.node(),
        "experiment": experiment.copy(),
        "start": datetime.utcnow().isoformat(),
        "status": None,
        "deviated": False,
        "steady_states": {
            "before": None,
            "after": None,
            "during": []
        },
        "run": [],
        "rollbacks": []
    }


def get_background_pools(experiment: Experiment) -> ThreadPoolExecutor:
    """
    Create a pool for background activities. The pool is as big as the number
    of declared background activities. If none are declared, returned `None`.
    """
    method = experiment.get("method", [])
    rollbacks = experiment.get("rollbacks", [])

    activity_background_count = 0
    for activity in method:
        if activity and activity.get("background"):
            activity_background_count = activity_background_count + 1

    activity_pool = None
    if activity_background_count:
        logger.debug(
            "{c} activities will be run in the background".format(
                c=activity_background_count))
        activity_pool = ThreadPoolExecutor(activity_background_count)

    rollback_background_pool = 0
    for activity in rollbacks:
        if activity and activity.get("background"):
            rollback_background_pool = rollback_background_pool + 1

    rollback_pool = None
    if rollback_background_pool:
        logger.debug(
            "{c} rollbacks will be run in the background".format(
                c=rollback_background_pool))
        rollback_pool = ThreadPoolExecutor(rollback_background_pool)

    return activity_pool, rollback_pool


def get_hypothesis_pool() -> ThreadPoolExecutor:
    """
    Create a pool for running the steady-state hypothesis continuously in the
    background of the method. The pool is not bounded because we don't know
    how long it will run for.
    """
    return ThreadPoolExecutor(max_workers=1)


def run_hypothesis_continuously(event: threading.Event, schedule: Schedule,
                                experiment: Experiment, journal: Journal,
                                configuration: Configuration,
                                secrets: Secrets,
                                event_registry: EventHandlerRegistry,
                                dry: bool = False):
    frequency = schedule.continous_hypothesis_frequency
    fail_fast_ratio = schedule.fail_fast_ratio

    event_registry.start_continous_hypothesis(frequency)
    logger.debug(
        "Executing the steady-state hypothesis continously "
        "every {} seconds".format(frequency))

    failed_iteration = 0
    failed_ratio = 0
    iteration = 1
    while not event.is_set():
        # already marked as terminated, let's exit now
        if journal["status"] in ["failed", "interrupted", "aborted"]:
            break

        state = run_steady_state_hypothesis(
            experiment, configuration, secrets, dry=dry)
        journal["steady_states"]["during"].append(state)
        event_registry.continous_hypothesis_iteration(iteration, state)

        if state is not None and not state["steady_state_met"]:
            failed_iteration += 1
            failed_ratio = (failed_iteration * 100) / iteration
            p = state["probes"][-1]
            logger.warning(
                "Continous steady state probe '{p}' is not in the given "
                "tolerance".format(p=p["activity"]["name"]))

            if schedule.fail_fast:
                if failed_ratio >= fail_fast_ratio:
                    m = "Terminating immediatly the experiment"
                    if failed_ratio != 0.0:
                        m = "{} after {:.1f}% hypothesis deviated".format(
                            m, failed_ratio
                        )
                    logger.info(m)
                    journal["status"] = "failed"
                    break

        # we do not adjust the frequency based on the time taken by probes
        # above. We really want frequency seconds between two iteration
        # not frequency as a total time of a single iteration
        event.wait(timeout=frequency)


def apply_activities(experiment: Experiment, configuration: Configuration,
                     secrets: Secrets, pool: ThreadPoolExecutor,
                     journal: Journal, dry: bool = False) -> List[Run]:
    with controls(level="method", experiment=experiment, context=experiment,
                  configuration=configuration, secrets=secrets) as control:
        runs = []
        for run in run_activities(
                experiment, configuration, secrets, pool, dry):
            runs.append(run)
            if journal["status"] in ["aborted", "failed", "interrupted"]:
                break

        if pool:
            logger.debug("Waiting for background activities to complete...")
            pool.shutdown(wait=True)

        result = []
        for run in runs:
            if not run:
                continue
            if isinstance(run, dict):
                result.append(run)
            else:
                result.append(run.result())

        control.with_state(result)

    return result


def apply_rollbacks(experiment: Experiment, configuration: Configuration,
                    secrets: Secrets, pool: ThreadPoolExecutor,
                    dry: bool = False) -> List[Run]:
    logger.info("Let's rollback...")
    with controls(level="rollback", experiment=experiment, context=experiment,
                  configuration=configuration, secrets=secrets) as control:
        rollbacks = list(
            run_rollbacks(experiment, configuration, secrets, pool, dry))

        if pool:
            logger.debug("Waiting for background rollbacks to complete...")
            pool.shutdown(wait=True)

        result = []
        for rollback in rollbacks:
            if not rollback:
                continue
            if isinstance(rollback, dict):
                result.append(rollback)
            else:
                result.append(rollback.result())

        control.with_state(result)

    return result
