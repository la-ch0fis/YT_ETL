import requests
import pytest
import psycopg2

def test_youtube_api_response(airflow_variable):
    """
    airflow_variable -> Gets the ficture as an argument
    url -> 
    """
    api_key = airflow_variable("api_key")
    channel_handle = airflow_variable("channel_handle")
    # This URL was defined on the API section under the 'dags' folder. The code (video_stats.py) is getting the playlist id. 
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={channel_handle}&key={api_key}"
    # try-except block: The porpuse is to catch any exceptions the real API (not mocked) can throw like a time-out error, which can happen in real life
    try:
        response = requests.get(url)
        assert response.status_code == 200
    except requests.RequestException as e:
        pytest.fail(f"Request to youtube API failed: {e}")


def test_real_postgres_connection(real_postgres_connection):
    """
    This function will use the fixture we defined in conftest.py
    A cursor variable will be initialized as None which will help with the closing logic in the final block.
    To make sure the connection works fine, a simple SQL statement will be executed.
    cursor.fetchone() method to get one row back.
    Error handlig: By using psycopg2.Error & pytest.fail() the error message will be clearer and controlled, not just a raw crash.
    If the end result of the test is PASSED, it means we're using hte correct credentials and the SQL statement ran as expected.
    """
    cursor = None
    try:
        cursor = real_postgres_connection.cursor()
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        assert result[0] == 1
    except psycopg2.Error as e:
        pytest.fail(f"Database query failed {e}")
    finally:
        if cursor is not None:
            cursor.close()



