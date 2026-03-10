# In this script we'll put the reusable code that will provide inputs for the test functions.
# We will have common setup code like databse connections, API keys and more.
# This reusable setup code is known as "fixtures" and to create such "fixture" we will use "Pytest fixture docorator".
# See for reference: https://docs.pytest.org/en/stable/getting-started.html
# conftest.py ---> https://docs.pytest.org/en/stable/reference/fixtures.html
import os
import pytest
from unittest import mock
from airflow.models import Variable

# First we will mock the API key
# We use the fixture decorator that will indicate this function is a fixture and will be used to provide input data for the main test
@pytest.fixture
def api_key():
    """
    This piece is temporarily updating the environment dictionary with key-value pais the we have here: AIRFLOW_VAR_API_KEY="MOCK_KEY1234"
    Finaly we use "yield" to provide the value to the test that request this fixture.
    """
    with mock.api.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY") 