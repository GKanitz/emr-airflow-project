import mmap
import re
import pandas as pd
from fastparquet import write

def read_dict(map_file, begin, pattern, col1Name = "Col1", col2Name = "Col2"):
    values = {col1Name: [], col2Name: []}
    p = map_file.find(begin)
    if p != -1:
        map_file.seek(p)
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
            values[col1Name].append(key)
            values[col2Name].append(value)
    return values

with open("I94_SAS_Labels_Descriptions.SAS", "r") as f:
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)

    i94cntyl = read_dict(mm, b'value i94cntyl', r"\s*(\d+)\s*=\s*'{1}(.+)'{1}", "Code", "Country")
    df = pd.DataFrame(data=i94cntyl)
    write('countryCodes.parquet', df)

    i94prtl = read_dict(mm, b'value $i94prtl', r"\s*'{1}(.+)'{1}\s*=\s*'{1}(.+)'{1}", "Code", "Port")
    df = pd.DataFrame(data=i94cntyl)
    write('portCodes.parquet', df)

    i94model = read_dict(mm, b'value i94model', r"\s*(\d+)\s*=\s*'{1}(.+)'{1}", "Code", "Transport")
    df = pd.DataFrame(data=i94cntyl)
    write('transportCodes.parquet', df)

    i94addrl = read_dict(mm, b'value i94addrl', r"\s*'{1}(.+)'{1}\s*=\s*'{1}(.+)'{1}", "Code", "State")
    df = pd.DataFrame(data=i94cntyl)
    write('stateCodes.parquet', df)

    i94visa = read_dict(mm, b'I94VISA', r"\s*(\d+)\s*=\s*(\w+)", "Code", "Visa")
    df = pd.DataFrame(data=i94cntyl)
    write('visaCodes.parquet', df)
