import json
import os.path as osp
import pandas as pd
import logging

def read_json(opt_path):
    """
    Function to load a json file as a dictionary

    Parameters
    ----------
    args: List[str]
        input path to the json file to be read, separate arguments will be combined in to a single path: ie foo, bar, test.json -> foo/bar/test.json    

    Returns
    -------
    data: Dict[str, ?]
        json file as a dictionary
    """
    path = osp.join(opt_path)
    with open(path, 'r') as f:
        data = json.load(f)
    return data

def write_json(data: dict, *args, indent:int = 4, **kwargs) -> None:
    """
    Function to write a dictionary out to a json file --- according to the json standards, the dictionary keys must be strings
    Parameters 
    ----------
    data : dict
        dictionary to be written to a json file
    args : str
        path for the file to be written to
    kwargs : dict
        additional keyword arguments to json.dump
    """

    out_path = osp.join(*args)

    with open(out_path, 'w') as fp:
        json.dump(data, fp, indent=indent, **kwargs)

    return


class LowCountsFullData(Exception):
    def __init__(self, full_rows_count) -> None:
        self.full_rows_count = full_rows_count

    def __str__(self):
        return f"{self.full_rows_count} rows of full data. It is recommended to increase your sampling dates"

class NoCountsFullData(Exception):
    def __init__(self, full_rows_count) -> None:
        self.full_rows_count = full_rows_count

    def __str__(self):
        return f"No Counts of full data. Please resample with greater date range"


class HighPercentageMissingTag(Exception):
    def __init__(self, high_missing_tag, missing_tag_value) -> None:
        self.high_missing_tag = missing_tag_value

    def __str__(self):
        return f"({self.high_missing_tag}) is missing {self.missing_tag_value}"


def check_low_counts(hist_data):
    """
    Function that checks number of rows with full data in historical data
    ----------
    hist_data : pd.DataFrame
        DataFrame of historical data
    """
    logger = logging.getLogger(__name__)

    trim_df = hist_data.dropna().copy()
    full_rows_count = trim_df.shape[0]

    try:
        if full_rows_count == 0:
            raise(NoCountsFullData(full_rows_count))
        elif full_rows_count < 4 and full_rows_count > 0:
            raise(LowCountsFullData(full_rows_count))
        
    except LowCountsFullData as error:
        logger.warning(str(error))

    return True