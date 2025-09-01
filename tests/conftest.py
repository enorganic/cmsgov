
import pytest
from dotenv import load_dotenv

from cmsgov.provider_data.v1.client import Client as ProviderDataClient

load_dotenv()


@pytest.fixture(name="client", autouse=True, scope="session")
def get_provider_data_client() -> ProviderDataClient:
    return ProviderDataClient(echo=True)
