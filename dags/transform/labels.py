"""Extract all label data into parquet files"""
import os
import re
import pandas as pd
from fastparquet import write
from smart_open import open


def read_dict(source_file, begin, pattern, key_col_name="Col1", val_col_name="Col2"):
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
    key_type = None
    value_type = None
    with open(source_file, "r") as f:
        line_number = 0
        for line in f:
            line_number = line_number + 1
            if line.find(begin) != -1:
                print(f"beginning found on line: {line_number}")
                break
        for line in f:
            match = re.match(pattern, line)
            if match is None:
                print(f"Read {len(values[key_col_name])} items")
                break

            group = match.groups()
            key = group[0].strip()
            value = group[1].strip()
            if key_type is None:
                if key.isdigit():
                    key_type = "int"
                else:
                    key_type = "str"
            if value_type is None:
                if value.isdigit():
                    value_type = "int"
                else:
                    value_type = "str"

            if key_type == "int":
                key = int(key)
            if value_type == "int":
                value = int(value)
            values[key_col_name].append(key)
            values[val_col_name].append(value)
        return values


source_file = os.path.join(source_bucket, "I94_SAS_Labels_Descriptions.SAS")

print("countryCodes")
data = read_dict(
    source_file, "value i94cntyl", r"\s*(\d+)\s*=\s*'{1}(.+)'{1}", "Code", "Country"
)
df = pd.DataFrame(data=data)
write(os.path.join(output_bucket, "countryCodes.parquet"), df, open_with=open)

print("portCodes")
data = read_dict(
    source_file, "value $i94prtl", r"\s*'{1}(.+)'{1}\s*=\s*'{1}(.+)'{1}", "Code", "Port"
)
df = pd.DataFrame(data=data)
write(os.path.join(output_bucket, "portCodes.parquet"), df, open_with=open)

print("transportCodes")
data = read_dict(
    source_file, "value i94model", r"\s*(\d+)\s*=\s*'{1}(.+)'{1}", "Code", "Transport"
)
df = pd.DataFrame(data=data)
write(os.path.join(output_bucket, "transportCodes.parquet"), df, open_with=open)

print("stateCodes")
data = read_dict(
    source_file,
    "value i94addrl",
    r"\s*'{1}(.+)'{1}\s*=\s*'{1}(.+)'{1}",
    "Code",
    "State",
)
df = pd.DataFrame(data=data)
write(os.path.join(output_bucket, "stateCodes.parquet"), df, open_with=open)

print("visaCodes")
data = read_dict(source_file, "I94VISA", r"\s*(\d+)\s*=\s*(\w+)", "Code", "Visa")
df = pd.DataFrame(data=data)
write(os.path.join(output_bucket, "visaCodes.parquet"), df, open_with=open)
