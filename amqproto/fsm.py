import re
import typing

Event = typing.NewType('Event', str)
State = typing.NewType('State', str)


class Transition:

    def __init__(self, event: Event, source: State, dest: State):
        self.event = event
        self.source = source
        self.dest = dest

        self._source_matcher = re.compile(source)

    def match_source(self, source: State):
        return self._source_matcher.match(source)

    def __repr__(self):
        return "<Transition from '{}' to '{}' by '{}'>".format(
            self.source, self.dest, self.event
        )


class Machine:

    def __init__(self,
                 transitions: typing.List[Transition],
                 states: typing.List[State],
                 initial_state: State):
        assert initial_state in states
        self._state = initial_state
        self._states = states
        self._transitions = {t.event: t for t in transitions}

    def trigger(self, event: Event):
        assert event in self._transitions
        transition = self._transitions[event]
        if not transition.match_source(self.current_state):
            raise RuntimeError("Can't trigger event {} from state {}".format(
                event, self.current_state
            ))
        self._state = transition.dest

    @property
    def current_state(self):
        return self._state
