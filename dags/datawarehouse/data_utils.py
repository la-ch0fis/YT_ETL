from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

table = "yt_api"


"""
    To interact with the database using Python we'll use a ver common adapter called Psycopg2
    Then we will need a package from Psycopg2 that is called RealDictCursor which allows the retrieval of reports using Python dictionaries 
    and not default tuples.
"""

def get_conn_cursor():
    """
        We set up the Postgres hook by defining a variable hook with 2 arguments: posgres_conn_id and database
        We defined this already in the docker-compose, you'll find it as AIRFLOW_CONN_POSTGRES_DB_YT_ELT.
        The second argument called database you'll find it in the .env file under the databse details.
        Now that we defined the hook we proceed to define the connection using the get_conn method.
        We also need to define the cursor by specifying the cursor method with the argument: cursfor_factory=RealDictCursor
    """
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # NOTE: Make sure to always close the cursor and the connection to free up resources
    # For popuses of making the code more modular, I'll create a separate function to close the cursor and connection
    # cur.close()
    # conn.close()
    return conn, cur


def close_conn_cursor(conn, cur):
    cur.close()
    conn.close()


def create_schema(schema):
    conn, cur = get_conn_cursor()

    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"

    cur.execute(schema_sql)
    conn.commit()

    close_conn_cursor(conn, cur)


def create_table(schema):
    conn, cur = get_conn_cursor()

    if schema == "staging":
        table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {schema}.{table} (
                        "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                        "Video_Title" TEXT NOT NULL,
                        "Upload_Date" TIMESTAMP NOT NULL,
                        "Duration" VARCHAR(20) NOT NULL,
                        "Video_Views" INT,
                        "Likes_Count" INT,
                        "Comments_Count" INT
                    );
                """
    else:
        table_sql = f"""
                     CREATE TABLE IF NOT EXISTS {schema}.{table} (
                         "Video_ID" VARCHAR(11) PRIMARY KEY NOT NULL,
                         "Video_Title" TEXT NOT NULL,
                         "Upload_Date" TIMESTAMP NOT NULL,
                         "Duration" VARCHAR(20) NOT NULL,
                         "Video_Type" VARCHAR(10) NOT NULL,
                         "Video_Views" INT,
                         "Likes_Count" INT,
                         "Comments_Count" INT
                     );
                """
    # Now, we execute the SQL statements for the table creation
    cur.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cur)


def get_video_ids(cur, schema):
    cur.execute(f"""SELECT "Video_ID" FROM {schema}.{table};""")
    # Fetch the ids
    ids = cur.fetchall()
    # We're interested only in the ID, so we use a list comprehension in order to retrieve just the IDs
    video_ids = [row['Video_ID'] for row in ids]

    return video_ids

