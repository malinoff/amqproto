def pytest_addoption(parser):
    parser.addoption('--stress', action='store_true')


def pytest_ignore_collect(path, config):
    if not config.getoption('stress') and 'stress' in str(path):
        return True
