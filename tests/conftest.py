def pytest_addoption(parser):
    parser.addoption(
        '--integration',
        action='store_true',
        help='run integration tests',
    )


def pytest_ignore_collect(path, config):
    if not config.getoption('integration') and 'integration' in str(path):
        return True
