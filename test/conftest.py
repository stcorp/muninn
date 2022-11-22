def pytest_addoption(parser):
    parser.addoption(
        "--config", action="store", default="test.cfg", help="test config file"
    )

def pytest_configure(config):
    global test_config
    test_config = config.getoption('--config')
