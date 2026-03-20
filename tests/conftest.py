# In this script we'll put the reusable code that will provide inputs for the test functions.
# We will have common setup code like databse connections, API keys and more.
# This reusable setup code is known as "fixtures" and to create such "fixture" we will use "Pytest fixture docorator".
# See for reference: https://docs.pytest.org/en/stable/getting-started.html
# conftest.py ---> https://docs.pytest.org/en/stable/reference/fixtures.html
import os
import pytest
import psycopg2
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
"""
This is a FACTORY PATTERN inside a fixture.
THE BURGER STAND ANALOGY 🍔

Element	                        What It Is	                 Analogy
@pytest.fixture	                The restaurant	             The place that makes food
def airflow_variable():	        The restaurant itself	     The building where burgers are made
def get_airflow_variable(...):	The burger maker             The PERSON who actually makes your burger
return get_airflow_variable	    Giving you the burger maker	 Here's the guy who makes burgers, YOU tell him what to make
WHAT'S ACTUALLY HAPPENING
Step 1: Pytest sees @pytest.fixture and says "OK, I'll run this function when tests need it."
Step 2: The outer function (airflow_variable) runs ONCE when the fixture is requested.
Step 3: BUT instead of returning a VALUE, it returns a FUNCTION (get_airflow_variable).
Step 4: Now your test gets that INNER function. You call it with different variable names, and it fetches different env vars.

WHY DO THIS INSTEAD OF JUST A REGULAR FUNCTION?
Approach                  What Happens                                  Problem
Regular function          get_env_var("DB_PASS")                        Works fine, but not a "fixture" – can't use dependency injection
Fixture returning value	  return os.getenv("SOMETHING")	                Only returns ONE hardcoded value
THIS NESTED APPROACH      Returns a FUNCTION that can return ANY value	You get FLEXIBILITY + FIXTURE MAGIC

"""
@pytest.fixture
def airflow_variable():
    """
    How to use it:
    def test_database_connection(airflow_variable):
        -> airflow_variable is actually the INNER function
        db_pass = airflow_variable("DB_PASSWORD")  Gets AIRFLOW_VAR_DB_PASSWORD
        db_user = airflow_variable("DB_USER")      Gets AIRFLOW_VAR_DB_USER
    """
    def get_airflow_variable(variable_name):
        """
        This function will return the value of the variables from the environment by specifying the varialbe name as an argument.
        """
        env_var = f"AIRFLOW_VAR_{variable_name.upper()}"
        return os.getenv(env_var)
    return get_airflow_variable


@pytest.fixture
def real_postgres_connection():
    """
    Function that will get the real credentials for the db connection
    The connection to PostgreSQL db will be established by using the Psycopg2 library.
    --> import it to use it
    """
    dbname = os.getenv("ELT_DATABASE_NAME")
    user = os.getenv("ELT_DATABASE_USERNAME")
    password = os.getenv("ELT_DATABASE_PASSWORD")
    host = os.getenv("POSTGRES_CONN_HOST")
    port = os.getenv("POSTGRES_CONN_PORT")
    
    conn = None

    try:
        conn = psycopg2.connect(
            dbname=dbname, 
            user=user,
            password=password,
            host=host,
            port=port
        )
        yield conn
    except psycopg2.Error as e:
        pytest.fail(f"Failed to connect to the database: {e}")
    finally: # MAKE SURE TO CLOSE THE CONNECTION
        if conn:
            conn.close()

        
