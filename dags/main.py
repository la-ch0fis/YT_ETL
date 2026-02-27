from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from api.video_stats import get_playlist_id, get_video_ids, extract_video_data, save_to_json

# pendulum library is used for dealing with timezones
# Define the local time-zone
local_tz = pendulum.timezone("America/Mexico_City")

# Default Args
default_args = {
    "owner":"BigPapi",
    "depends_on_past":False,
    "email_on_failure":False,
    "email_on_retry":False,
    "email":"ulises.rangel@gmail.com",
    #"retries": 1,
    #"retry_delay":timedelta(minutes=5),
    "max_active_runs":1,
    "dagrun_timeout":timedelta(hours=1),
    "start_date":datetime(2026,1,1, tzinfo=local_tz),
    # "end_date":datetime(2026,3,10, tzinfo=local_tz)
    
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to retrieve JSON raw data",
    schedule="0 15 * * *",
    catchup=False   
) as dag:
    #Define tasks
    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extracted_data = extract_video_data(video_ids)
    save_to_json_raw = save_to_json(extracted_data)

    #Define task dependencies
    playlist_id >> video_ids >> extracted_data >> save_to_json_raw