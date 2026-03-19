# In this script we'll put the reusable code that will provide inputs for the test functions.
# We will have common setup code like databse connections, API keys and more.
# This reusable setup code is known as "fixtures" and to create such "fixture" we will use "Pytest fixture docorator".
# See for reference: https://docs.pytest.org/en/stable/getting-started.html
# conftest.py ---> https://docs.pytest.org/en/stable/reference/fixtures.html
import os
import pytest
from unittest import mock
from airflow.models import Variable, Connection, DagBag


# First we will mock the API key
# We use the fixture decorator that will indicate this function is a fixture and will be used to provide input data for the main test
@pytest.fixture
def api_key():
    """
    This piece is temporarily updating the environment dictionary with key-value pairs that we have here: AIRFLOW_VAR_API_KEY="MOCK_KEY1234"
    Finaly we use "yield" to provide the value to the test that request this fixture.
    As we saw in the Airflow section, the 'Variable.get()' fetches the value of any airflow variable and in this case we want the 'API_KEY' variable.
    mock.patch.dict is where the lie-magic happens. What it does is to create a temporary fake version of 'os.environ' instead of actually change the 
    computer's real environment variables. 
    """
    with mock.patch.dict("os.environ", AIRFLOW_VAR_API_KEY="MOCK_KEY1234"):
        yield Variable.get("API_KEY") 


@pytest.fixture
def channel_handle():
    """
    This piece is temporarily updating the environment dictionary with key-value pairs that we have here: AIRFLOW_VAR_API_KEY="MOCK_KEY1234"
    Finaly we use "yield" to provide the value to the test that request this fixture.
    """
    with mock.patch.dict("os.environ", AIRFLOW_VAR_CHANNEL_HANDLE="Koala.puffss"):
        yield Variable.get("CHANNEL_HANDLE") 

# hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
@pytest.fixture
def mock_postgres_conn_vars():
    conn = Connection(
        login="mock_username",
        password="mock_password",
        host="mock_host",
        port=1234,
        schema="mock_db_name"  # schema is the db name
    )
    conn_uri = conn.get_uri()

    with mock.patch.dict("os.environ", AIRFLOW_CONN_POSTGRES_DB_YT_ELT=conn_uri):
        yield Connection.get_connection_from_secrets(conn_id="POSTGRES_DB_YT_ELT")


@pytest.fixture
def dagbag():
    """
    This will help us test our DAGs structure. 
    To interact with the DAGs we need to import the DagBag class at the top which will collect all the DAGs information.
    """
    yield DagBag()


# INTEGRATION TESTING PART: WE NEED TO MAKE SURE THAT THE DIFFERENT PARTS OF THE PROJECT/SYSTEM WORK TOGETHER, WHICH IN THIS CASE IS THE ELT PIPELINE 
# A quick example of this would be the part where we transform the data from STAGING to CORE layer and sove it into the postgres table.
# So, here what we want is to test that the transformed data, which we transformed using the python functions is updated correctly into the postgres DB.
# Now, we'll aim to use real credentials for integration testing whcih makes sense becasue we need to test the db connection and API response.
# These two can bahave different, specially when we're mocking functionality, we need to consider latency and possible API failures.
@pytest.fixture
def airflow_variable():
    def get_airflow_variable(variable_name):
        """
        This function will return the value of the variables from the environment by specifying the varialbe name as an argument.
        """
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    return get_airflow_variable
