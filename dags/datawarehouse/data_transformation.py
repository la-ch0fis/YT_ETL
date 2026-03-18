from datetime import timedelta, datetime


def parse_duration(duration_str):
    """
       Now let's tranform some data. The duration of the videos in yutu is specified using a standard: ISO 8601
       For example, for videos that last at least 1 minute and less than 1 hour use this format: PT#M#S. In whcih the letters PT indicate that the value
       specified is a period of time and the letters M and S refer to length in minutes and seconds respectively. The # characters preceding M and S letters are 
       both integers that specify the number of minutes or seconds of the video. If the video is at least 1 hour long, the durantion is in the format PT#H#M#S
       Let's code this, Cochise! And do not forget to keep checking https://developers.google.com/youtube/v3 for any questions on how the API works.
    """
    duration_str = duration_str.replace('P', '').replace('T', '') # Getting rid of the PT 
    components = ['D','H','M','S'] # List with -> D: Day,H: Hour ,M: Minute, S: Second 
    
    values = { 'D':0,
               'H':0,
               'M':0,
               'S':0
            }
    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    total_duration = timedelta(
        days=values['D'],
        hours=values['H'],
        minutes=values['M'],
        seconds=values['S']
    )
    # timedelta function: It will return a time delta object whcih python represents a duration or the difference between 2 dates or times.
    return total_duration

# print(parse_duration('PT38M21S'))


def transform_data(row):
    duration_td = parse_duration(row["Duration"])
    row["Duration"] = (datetime.min + duration_td).time() # datetime.min is the earliest representable datetime in Python.
    # print(datetime.min) --> 0001-01-01 00:00:00 -or- print(date.min) --> 0001-01-01 <-- for this we need to import "date" also.
    # Now, let's define the Video Type based on the "Duration" we just extracted. Less than a minute is a short and more then 1 min is a normal video.
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row
     