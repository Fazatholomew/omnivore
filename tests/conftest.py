import pytest

from omnivore.utils.logging import getLogger

LOGGER = getLogger(__name__)


@pytest.fixture(scope="function")
def example_fixture():
    LOGGER.info("Setting Up Example Fixture...")
    yield
    LOGGER.info("Tearing Down Example Fixture...")
