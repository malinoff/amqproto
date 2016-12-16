import re
import typing
from collections import defaultdict

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


class FunctionalMachine:

    def __init__(self,
                 name,
                 transitions: typing.List[Transition],
                 states: typing.List[State],
                 initial_state: State):
        assert initial_state in states
        self.name = name
        self._state = initial_state
        self._states = states
        self._transitions = defaultdict(list)
        for transition in transitions:
            self._transitions[transition.event].append(transition)

    def trigger(self, event: Event):
        assert event in self._transitions, event
        transitions = self._transitions[event]
        for transition in transitions:
            if transition.match_source(self.current_state):
                self._state = transition.dest
                break
        else:
            raise RuntimeError("Can't trigger event {} from state {}".format(
                event, self.current_state,
            ))

    @property
    def current_state(self):
        return self._state


#class Machine(type):
#
#    def __prepare__(metacls, name, bases, **kwds):
#        for name, obj in kwds.items():
#            if issubclass(obj, me
