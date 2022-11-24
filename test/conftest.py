import os

def pytest_addoption(parser):
    parser.addoption(
        "--config", action="store", default="test.cfg", help="test config file"
    )

def pytest_configure(config):
    os.environ['MUNINN_TEST_CFG'] = config.getoption('--config')
