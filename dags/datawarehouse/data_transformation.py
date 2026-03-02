from datetime import timedelta


def parse_duration(duration_str):
    """
       Now let's tranform some shit, cuz. The duration of the videos in youtu is specified using a standard: ISO 8601
       For example, for videos that last at least 1 minute and less than 1 hour use this format: PT#M#S. In whcih the letters PT indicate that the value
       specified is a period of time and the letters M and S refer to length in minutes and seconds respectively. The # characters preceding M and S letters are 
       both integers that specify the number of minutes or seconds of the video. If the video is at least 1 hour long, the durantion is in the format PT#H#M#S
       Let's code this shit, Cochise!
    """
    duration_str = duration_str.replace('P', '').replace('T', '')
    components = ['D','H','M','S']
    
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

    return total_duration


# print(parse_duration('PT38M21S'))
