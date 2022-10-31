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

class TagMissingInMapper(Exception):
    def __init__(self, tag) -> None:
        self.tag = tag

    def __str__(self):
        return f"Tag ({self.tag}) present in tag_list but not in Mapper"



def check_missing_tags(tag_df, mapper):
    """
    Function check for missing tags
    Parameters
    ----------
    tag_df : pd.DataFrame
        DataFrame of tags in their original name format
    mapper: pd.DataFrame
        Dataframe of mapper
    """

    logger = logging.getLogger(__name__)

    for short_name in tag_df['Tags'].tolist():
        try:
            if not any([True if short_name in long_name else False for long_name in mapper['Datapoint Name'].tolist()]):
                raise(TagMissingInMapper(short_name))
        except TagMissingInMapper as error:
            logger.warning(str(error))

    return True

def short_to_long_and_missing_tags(tag_df, mapper):
    """
    Function to convert short (original) tags to the longer format from the mapper
    Parameters
    ----------
    tag_df : pd.DataFrame
        DataFrame of tags in their original name format
    mapper: pd.DataFrame
        Dataframe of mapper
    Returns
    -------
    new_tags : list
        tag names from mapper
    """
    new_tags = []
    missing_tags = []

    for short_name in tag_df['Tags'].tolist():
        for long_name in mapper['Datapoint Name'].tolist():
            if short_name in long_name:
                new_tags.append(long_name)
        
        if not any([True if short_name in long_name else False for long_name in mapper['Datapoint Name'].tolist()]):
            missing_tags.append(short_name)

    return new_tags, missing_tags




def missing_tags_in_mapper(tag_df, mapper):
    """
    Function to gather all tags that are not in mapper
    Parameters
    ----------
    tag_df : pd.DataFrame
        DataFrame of tags in their original name format
    mapper: pd.DataFrame
        Dataframe of mapper
    Returns
    -------
    missing_tags : list
        tag names that are not in mapper
    """
    


def delete_duplicate_tags(tag_df):
    """
    Function to delete any duplicate tags
    Parameters
    ----------
    tag_df : pd.DataFrame
        DataFrame of tags in their original name format
    Returns
    -------
    slim_df : pd.DataFrame
        DataFrame that does not contain duplicates
    """
    slim_df = tag_df.drop_duplicates(subset = ['Tags']).copy()

    return slim_df

