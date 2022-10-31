import json
import os.path as osp
import pandas as pd
import pickle

FILE_TYPES = ['csv', 'xlsx', 'json', 'txt']

class UnsupportedFileType(Exception):
    def __init__(self, extension):
        self.extension = extension

    def __str__(self) -> str:
        return f"{self.extension} is not supported. Supported file types are: {', '.join(FILE_TYPES)}"

def read_json(*args: str):
    """
    Function to load a json file as a dictionary

    Parameters
    ----------
    args: List[str] | str
        input path to the json file to be read, separate arguments will be combined in to a single path: ie foo, bar, test.json -> foo/bar/test.json    

    Returns
    -------
    data: dict
        json file as a dictionary
    """
    path = osp.join(*args)
    with open(path, 'r') as f:
        data = json.load(f)
    return data

def read_file(file_path:str, date_label:str, *args, **kwargs) -> pd.DataFrame:
    """
    High level function to read in data from a few different file types

    Parameters
    ----------
    file_path : str
        path to the file to read
    date_label : str
        name of the date label
    args : List[?]
        additonal arguments to the read function
    kwargs : Dict[str] -> ?
        additional kwargs to the read function

    Returns : List[str]
        list of tags (object names)
    """
    extension = file_path.split('.')[-1]
    # check if the extension is supported
    if extension not in FILE_TYPES:
        raise UnsupportedFileType(extension=extension)

    if extension == 'csv' or extension == 'txt':
        return pd.read_csv(file_path, *args, **kwargs).set_index(date_label)
    elif extension == 'xlsx':
        return pd.read_excel(file_path, *args, **kwargs).set_index(date_label)
    # TODO: check the if this is the conversion we want to 
    return pd.DataFrame(read_json(file_path)).set_index(date_label)

def read_pickle(*args:str):
    """
    Function to read pickle files

    Parameters
    ----------
    args: List[str] | str
        input path to the json file to be read, separate arguments will be combined in to a single path: ie foo, bar, test.json -> foo/bar/test.pkl   

    Returns
    -------
    data : ?
        contents of the pickle file    
    """
    file_path = osp.join(*args)

    with open(file_path, 'rb') as fp:
        data = pickle.load(fp)

    return data
    