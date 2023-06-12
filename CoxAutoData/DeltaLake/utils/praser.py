from datetime import datetime
import re

def prase_EpochDate_str(name: str) -> str:
    return datetime.fromtimestamp(int(name)).strftime('%Y-%m-%d') if name.isdecimal() else None


def prase_filename_EpochDate(name: str, delimiter:str ='_')-> str:
    try:
        spl_name = name.split(delimiter)
        return list(filter(lambda x: re.match("\\d+",x),spl_name))[0]
    except IndexError:
        return None

