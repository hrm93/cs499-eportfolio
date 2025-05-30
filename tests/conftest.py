import pytest
import logging

@pytest.fixture(autouse=True)
def configure_logging():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(message)s')
