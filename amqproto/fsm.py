from transitions import Machine


class Connection(Machine):
    initial = 'CLOSED'
    states = [
        'CLOSED',
        'PROTOCOL',
        'START',
        'SECURE',
        'TUNE',
        'OPEN',
        'CLOSING',
    ]
    transitions = [
        {'trigger': 'initiate', 'source': 'CLOSED', 'dest': 'PROTOCOL'},
        {'trigger': 'start', 'source': 'PROTOCOL', 'dest': 'START'},
        {'trigger': 'secure', 'source': 'START', 'dest': 'SECURE'},
        {'trigger': 'tune', 'source': ['START', 'SECURE'], 'dest': 'TUNE'},
        {'trigger': 'open', 'source': 'TUNE', 'dest': 'OPEN'},
        {'trigger': 'close', 'source': 'OPEN', 'dest': 'CLOSING'},
        {'trigger': 'terminate', 'source': '*', 'dest': 'CLOSED'},
    ]

    def __init__(self):
        super().__init__(
            initial=self.initial, states=self.states,
            transitions=self.transitions,
        )


class Channel(Machine):
    initial = 'CLOSED'
    states = [
        'CLOSED',
        'OPENING',
        'OPEN',
        'CLOSING',
    ]
    transitions = [
        {'trigger': 'initiate', 'source': 'CLOSED', 'dest': 'OPENING'},
        {'trigger': 'open', 'source': 'OPENING', 'dest': 'OPEN'},
        {'trigger': 'close', 'source': 'OPEN', 'dest': 'CLOSING'},
        {'trigger': 'terminate', 'source': '*', 'dest': 'CLOSED'},
    ]

    def __init__(self):
        super().__init__(
            initial=self.initial, states=self.states,
            transitions=self.transitions,
        )
