# -*- coding: utf-8 -*-
from typing import NoReturn

from chaoslib.exceptions import ActivityFailed, InvalidActivity, \
    InvalidExperiment, InterruptExecution
from chaoslib.experiment import run_experiment
from chaoslib.run import RunEventHandler, Schedule, Strategy
from chaoslib.types import Experiment, Journal

from fixtures import experiments


def test_run_ssh_before_method_only():
    experiment = experiments.SimpleExperiment.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.BEFORE_METHOD)
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is None


def test_run_ssh_after_method_only():
    experiment = experiments.SimpleExperiment.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.AFTER_METHOD)
    assert journal is not None
    assert journal["steady_states"]["before"] is None
    assert journal["steady_states"]["after"] is not None


def test_run_ssh_default_strategy():
    experiment = experiments.SimpleExperiment.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.DEFAULT)
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is not None


def test_run_ssh_during_method_only():
    experiment = experiments.SimpleExperiment.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.DURING_METHOD)
    assert journal is not None
    assert journal["steady_states"]["before"] is None
    assert journal["steady_states"]["after"] is None
    assert journal["steady_states"]["during"] is not None


def test_run_ssh_continous():
    experiment = experiments.SimpleExperiment.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.CONTINOUS,
        schedule=Schedule(continous_hypothesis_frequency=0.1))
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is not None
    assert journal["steady_states"]["during"] is not None


def test_exit_continous_ssh_continous_when_experiment_is_interrupted():
    handlers_called = []
    class Handler(RunEventHandler):
        def started(self, experiment: Experiment,
                    journal: Journal) -> NoReturn:
            handlers_called.append("started")

        def interrupted(self, experiment: Experiment,
                        journal: Journal) -> NoReturn:
            handlers_called.append("interrupted")

    experiment = experiments.SimpleExperimentWithInterruption.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.CONTINOUS,
        schedule=Schedule(continous_hypothesis_frequency=0.1),
        event_handlers=[Handler()])
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is None
    assert journal["steady_states"]["during"] is not None
    assert journal["deviated"] == False
    assert journal["status"] == "interrupted"
    assert sorted(handlers_called) == ["interrupted", "started"]


def test_exit_continous_ssh_continous_when_experiment_is_exited():
    handlers_called = []
    class Handler(RunEventHandler):
        def started(self, experiment: Experiment,
                    journal: Journal) -> NoReturn:
            handlers_called.append("started")

        def interrupted(self, experiment: Experiment,
                        journal: Journal) -> NoReturn:
            handlers_called.append("interrupted")

    experiment = experiments.SimpleExperimentWithExit.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.CONTINOUS,
        schedule=Schedule(continous_hypothesis_frequency=0.1),
        event_handlers=[Handler()])
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is None
    assert journal["steady_states"]["during"] is not None
    assert journal["deviated"] == False
    assert journal["status"] == "interrupted"
    assert sorted(handlers_called) == ["started"]



def test_exit_continous_ssh_continous_when_activity_raises_unknown_exception():
    experiment = experiments.SimpleExperimentWithException.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.CONTINOUS,
        schedule=Schedule(continous_hypothesis_frequency=0.1))
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is not None
    assert journal["steady_states"]["during"] is not None
    assert journal["deviated"] == False
    assert journal["status"] == "completed"
    assert len(journal["run"]) == 2
    assert journal["run"][-1]["status"] == "failed"
    assert "oops" in journal["run"][-1]["exception"][-1]


def test_exit_immediatly_when_continous_ssh_fails_and_failfast():
    experiment = experiments.SimpleExperimentWithSSHFailingAtSomePoint.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.CONTINOUS,
        schedule=Schedule(continous_hypothesis_frequency=0.1, fail_fast=True))
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is not None
    assert journal["steady_states"]["during"] is not None
    assert journal["status"] == "failed"
    assert journal["deviated"] == True
    assert len(journal["run"]) == 1


def test_do_not_exit_when_continous_ssh_fails_and_no_failfast():
    experiment = experiments.SimpleExperimentWithSSHFailingAtSomePoint.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.CONTINOUS,
        schedule=Schedule(continous_hypothesis_frequency=0.1, fail_fast=False))
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is not None
    assert journal["steady_states"]["during"] is not None
    assert journal["status"] == "failed"
    assert journal["deviated"] == True
    assert len(journal["run"]) == 2


def test_exit_immediatly_when_continous_ssh_fails_and_failfast_when_background_activity():
    experiment = experiments.SimpleExperimentWithSSHFailingAtSomePointWithBackgroundActivity.copy()
    journal = run_experiment(
        experiment, strategy=Strategy.CONTINOUS,
        schedule=Schedule(continous_hypothesis_frequency=0.1, fail_fast=True))
    assert journal is not None
    assert journal["steady_states"]["before"] is not None
    assert journal["steady_states"]["after"] is not None
    assert journal["steady_states"]["during"] is not None
    assert journal["status"] == "failed"
    assert journal["deviated"] == True
    assert len(journal["run"]) == 2
