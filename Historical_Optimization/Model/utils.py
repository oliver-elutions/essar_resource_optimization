import pandas as pd
import numpy as np
import json
import os.path as osp
import pickle

def calculate_bounds(bounds, df):
    low_bound = []
    up_bound = []
    for k,v in bounds.items():
        low_bound.append(float(df[k] + v[0]))
        up_bound.append(float(df[k] + v[1]))
    
    return list(zip(low_bound, up_bound))


def obj_fun(ctrl, nonctrl, model, outlet, c=0.01):
    # TODO: standardize these variables to get on same scale
    # TODO: play with reduction (sum, mean, ...)
    data = np.array(np.concatenate([ctrl, nonctrl]).reshape(1, -1))
    oil = data[0][0]
    gas = data[0][1]
    model_output = model.predict(data)
    
    # two terms in the objective function:
    # 1. oil + gas which are the inputs ... want to get goo dbang for your buck
    # 2. min(model_output - outlet, 0)
    # 3. c = scaler to control weighting between 1 and 2
    return (oil + gas) + c * abs(model_output - outlet)

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