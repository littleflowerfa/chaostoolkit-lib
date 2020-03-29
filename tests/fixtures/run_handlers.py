from typing import Any, NoReturn
from chaoslib.run import RunEventHandler
from chaoslib.types import Experiment, Journal


class FullRunEventHandler(RunEventHandler):
    def __init__(self):
        self.calls = []

    def started(self, experiment: Experiment, journal: Journal) -> NoReturn:
        self.calls.append("started")

    def finish(self, journal: Journal) -> NoReturn:
        self.calls.append("finish")

    def interrupted(self, experiment: Experiment,
                    journal: Journal) -> NoReturn:
        self.calls.append("interrupted")

    def signal_exit(self) -> NoReturn:
        self.calls.append("signal_exit")

    def start_continous_hypothesis(self, frequency: int) -> NoReturn:
        self.calls.append("start_continous_hypothesis")

    def continous_hypothesis_iteration(self, iteration_index: int,
                                       state: Any) -> NoReturn:
        self.calls.append("continous_hypothesis_iteration")

    def continous_hypothesis_completed(self) -> NoReturn:
        self.calls.append("continous_hypothesis_completed")

    def start_method(self, iteration_index: int = 0) -> NoReturn:
        self.calls.append("start_method")

    def condition_completed(self, state: Any,
                            iteration_index: int = 0) -> NoReturn:
        self.calls.append("condition_completed")

    def start_cooldown(self, duration: int) -> NoReturn:
        self.calls.append("start_cooldown")

    def cooldown_completed(self) -> NoReturn:
        self.calls.append("cooldown_completed")


class FullExceptionRunEventHandler(RunEventHandler):
    def __init__(self):
        self.calls = []

    def started(self, experiment: Experiment, journal: Journal) -> NoReturn:
        raise Exception()

    def finish(self, journal: Journal) -> NoReturn:
        raise Exception()

    def interrupted(self, experiment: Experiment,
                    journal: Journal) -> NoReturn:
        raise Exception()

    def signal_exit(self) -> NoReturn:
        raise Exception()

    def start_continous_hypothesis(self, frequency: int) -> NoReturn:
        raise Exception()

    def continous_hypothesis_iteration(self, iteration_index: int,
                                       state: Any) -> NoReturn:
        raise Exception()

    def continous_hypothesis_completed(self) -> NoReturn:
        raise Exception()

    def start_method(self, iteration_index: int = 0) -> NoReturn:
        raise Exception()

    def condition_completed(self, state: Any,
                            iteration_index: int = 0) -> NoReturn:
        raise Exception()

    def start_cooldown(self, duration: int) -> NoReturn:
        raise Exception()

    def cooldown_completed(self) -> NoReturn:
        raise Exception()
