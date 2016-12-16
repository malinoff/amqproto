import asyncio
import pytest

@pytest.hookimpl(hookwrapper=True)
def pytest_fixture_setup(fixturedef, request):
    outcome = yield
    coro = outcome.get_result()
    if asyncio.iscoroutine(coro):
        event_loop = request.getfixturevalue('event_loop')
        result = event_loop.run_until_complete(coro)

        cr = fixturedef.cached_result
        fixturedef.cached_result = result, cr[1], cr[2]
        outcome.result = result
