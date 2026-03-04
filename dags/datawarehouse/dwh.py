from datawarehouse.data_utils import get_video_ids, close_conn_cursor, create_schema, create_table, get_conn_cursor
from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import parse_duration, transform_data
import logging
from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    """
        This function will create the actual staging table in our DWH that we'll be using for the ELT process, cuz.
    """
    schema = "staging"
    conn, cur = None, None
    try:
        conn, cur = get_conn_cursor()
        # We load the raw data extracted from the youtu API and saved as JSON file
        YT_data = load_data()
        # We create the schema and table for staging if they do not exist
        create_schema(schema)
        create_table(schema)
        # We define a variable to hold all the video IDs for the staging table by suing the get_video_ids function
        video_ids = get_video_ids(cur, schema)
        # Now we loop through each row and insert or update depending on certain conditions.
        for row in YT_data:
            # if it's the first insert meaning the table is empty we do:
            if len(video_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row["video_id"] in video_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
            
        # Now we just need to take care of possible removal of videos by the channel owner
        # We define a set variable called IDs JSON, which will give all the video IDs that are present in the new JSON data
        ids_in_json = {row["video_id"] for row in YT_data}
        # Now we define an IDs to delete variable. That will be the difference between the video IDs we have in the staging table and the above defined IDs in JSON
        # We'll now convert the table IDs to a python set, so we can use the minus operator to the difference between the 2 sets if there's any.
        ids_to_delete = set(video_ids) - ids_in_json
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        logger.info(f"{schema} table update completed")
    except Exception as e:
        logger.error(f"An error ocurred during the update of {schema} table: {e}")
        raise e
    # We need to finally close the fucking connection and the cursor to free up resources, this is very important, if this shit fails, the conn and cur will be closed
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


@task
def core_table():
    """
        This function will create the actual core table in our DTW
    """
    schema = "core"
    conn, cur = None, None

    try:
        conn, cur = get_conn_cursor()
        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)
        current_video_ids = set() # Callect all video IDs from current execution. Will be used for deletion logic
        # Now we get all the data from the staging table by using our cursor defined previously
        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()
        # NOTE: We're selecting all the data here, however, in real life we could have millions or records and this would be a wrong approach
        #       We would need leverage the batch processing. 
        # Now, we loop trhough all the rows and tranforms, insert or update, depending on certain conditions
        for row in rows:
            current_video_ids.add(row["Video_ID"])
            if len(table_ids) == 0: # Check if table is empty (which means is the first load of data), if yes we transform the row and insert into the table
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, rows)
            else: # If we already have data present in the table then we update it
                transformed_row = transform_data(row) # We transform the data
                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)
                else: # In case we are dealing with new data, we insert it
                    insert_rows(cur, conn, schema, transformed_row)
        # Now, in case the channel owner have deleted videos, we need to ger rid of them, so we define a list with the IDs to delete as the difference 
        # between what is añready in the table but not in the current execution.
        ids_to_delete = set(table_ids) - current_video_ids 
        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
        logger.info(f"{schema} table update completed")

    except Exception as e:
        logger.error(f"An error occurred during the update of {schema} table: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)
