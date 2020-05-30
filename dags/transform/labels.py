"""Extract all label data into parquet files"""
import mmap
import re
import pandas as pd
from fastparquet import write


def read_dict(map_file, begin, pattern, key_col_name="Col1", val_col_name="Col2"):
    """Create dictionary from text stream. Number values are converted to int.

    Arguments:
        map_file {mmap} -- open file
        begin {str} -- Pattern that indicates the begin of the list in file
        pattern {str} -- Regex pattern that is applied to each row to extract key value pairs

    Keyword Arguments:
        key_col_name {str} -- Key colum name (default: {"Col1"})
        val_col_name {str} -- Value column name (default: {"Col2"})

    Returns:
        dict -- Dictionary containing all extracted key value pairs
    """
    values = {key_col_name: [], val_col_name: []}
    position = map_file.find(begin)
    if position != -1:
        map_file.seek(position)
        map_file.readline()
        for line in iter(map_file.readline, b""):
            match = re.match(pattern, line.decode("utf-8"))
            if match is None:
                break

            group = match.groups()
            key = group[0].strip()
            if key.isdigit():
                key = int(key)
            value = group[1].strip()
            if value.isdigit():
                value = int(value)
            values[key_col_name].append(key)
            values[val_col_name].append(value)
    return values


with open("I94_SAS_Labels_Descriptions.SAS", "r") as f:
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

    i94cntyl = read_dict(
        mm, b"value i94cntyl", r"\s*(\d+)\s*=\s*'{1}(.+)'{1}", "Code", "Country"
    )
    df = pd.DataFrame(data=i94cntyl)
    write("countryCodes.parquet", df)

    i94prtl = read_dict(
        mm, b"value $i94prtl", r"\s*'{1}(.+)'{1}\s*=\s*'{1}(.+)'{1}", "Code", "Port"
    )
    df = pd.DataFrame(data=i94cntyl)
    write("portCodes.parquet", df)

    i94model = read_dict(
        mm, b"value i94model", r"\s*(\d+)\s*=\s*'{1}(.+)'{1}", "Code", "Transport"
    )
    df = pd.DataFrame(data=i94cntyl)
    write("transportCodes.parquet", df)

    i94addrl = read_dict(
        mm, b"value i94addrl", r"\s*'{1}(.+)'{1}\s*=\s*'{1}(.+)'{1}", "Code", "State"
    )
    df = pd.DataFrame(data=i94cntyl)
    write("stateCodes.parquet", df)

    i94visa = read_dict(mm, b"I94VISA", r"\s*(\d+)\s*=\s*(\w+)", "Code", "Visa")
    df = pd.DataFrame(data=i94cntyl)
    write("visaCodes.parquet", df)
