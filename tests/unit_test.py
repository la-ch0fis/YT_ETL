def test_api_key(api_key):
    """
        We create the function for the testing an it has to start with the prefix 'test_'
        and an appropiate name, in this case 'api_key'. This function will take as argument the API_KEY fixture defined in the conftest.py file.
    """
    assert api_key == "MOCK_KEY1234"


def test_channel_handle(channel_handle):
    """
        We create the function for the testing an it has to start with the prefix 'test_'
        and an appropiate name, in this case 'channel_handle'. This function will take as argument the channel_handle fixture defined in the conftest.py file.
    """
    assert channel_handle == "Koala.puffss"


def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "mock_db_name" 


def test_dags_integrity(dagbag):
    # 1. No import error check
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("==========================")
    print(dagbag.import_errors)
    # 2. All expected DAGs are loaded. First we gather all the DAG IDs in a list, then we get the actual DAG ids from the DagBag keys
    expected_dag_ids = ["produce_json", "update_db", "data_quality"]
    loaded_dag_ids = list(dagbag.dags.keys())
    # Remember, these DAG IDs were defined in the main.py script under the 'dags' directory.
    print("==========================")
    print(dagbag.import_errors)
    # Now, using a for-loop we go through each DAG ID in our expected DAG ID list, asserting that this DAG ID is found in the DAG keys.
    for dag_id in expected_dag_ids:
        assert dag_id in loaded_dag_ids, f"DAG {dag_id} is missing."
    # 3. Count of DAGs, this assertion should return 3
    assert dagbag.size() == 3
    print("==========================")
    print(f"Number of DAGs found: {dagbag.size()}")
    # 4. Finally we assert that all the DAGs have the expected number of tasks.
    # So, first we define a dictionary with key-value pairs being the DAG ID and the expected task count for each DAG
    expected_task_count = {
        "produce_json": 4,
        "update_db": 2,
        "data_quality": 2    
    }
    print("==========================")
    # for-loop to go through each DAG
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_count[dag_id]
        actual_count = len(dag.tasks)
        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}"
        
        print(f"DAG ID: {dag_id} - Num Tasks: {len(dag.tasks)}")