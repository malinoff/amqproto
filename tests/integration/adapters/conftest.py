import asyncio

import pytest


class ForbidGlobalPolicy(asyncio.AbstractEventLoopPolicy):
    pass


@pytest.fixture()
def loop():
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    # asyncio.set_event_loop_policy(ForbidGlobalPolicy())
    return loop
