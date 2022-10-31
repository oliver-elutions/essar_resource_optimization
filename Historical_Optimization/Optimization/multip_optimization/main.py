import argparse
import logging
import multiprocessing as mp
import os
import os.path as osp
from unittest import result
import pandas as pd
import datetime
from tqdm import tqdm

# local imports
from reader import read_file, read_json, read_pickle
from tools import mp_optimization

import warnings
warnings.filterwarnings('ignore')


def parse_args() -> argparse.Namespace:
    """Function to parse command line arguments"""
    parser = argparse.ArgumentParser()

    parser.add_argument('input_file', type=str, help='Path to the input file')
    parser.add_argument('out_path', type=str, help='Path to the output file (including the directory)')
    parser.add_argument('c_value', type=float, help='C value')
    parser.add_argument('--date-label', type=str, required=False, default='Date', help='Column Label for the date')
    parser.add_argument('--n-cores', type=int, required=False, default=mp.cpu_count(), help='Number of cores to use (default is all')
    parser.add_argument('--max-iter', type=int, required=False, default=75, help='Max iterations for dual annealing')
    parser.add_argument('--config-path', type=str, required=False, default='controllable.json', help='Path to the config file')
    parser.add_argument('--model-path', type=str, required=False, default='model.pkl', help='Path to model pickle file')


    args = parser.parse_args()
    return args


def main() -> None:
    """Main Function"""
    start = datetime.datetime.now()
    mp.set_start_method("spawn")
    args = parse_args()

    # set up logger
    formatstr = '%(asctime)s: %(levelname)s: %(funcName)s Line: %(lineno)d %(message)s'
    datestr = '%m/%d/%Y %H:%M:%S'
    logging.basicConfig(
        level=logging.INFO, 
        format=formatstr, 
        datefmt=datestr, 
        handlers=[
            logging.FileHandler('mp.log'),
            logging.StreamHandler()
            ]
        )

    assert args.n_cores > 1 and args.n_cores <= mp.cpu_count(), f"NumberOfCoresError: Number of cores must be greater than 1 but less than {mp.cpu_count()}. Recieved {args.n_cores}"

    logging.info("Reading Config and Data")
    
    # read in config
    config = read_json(args.config_path)
    controllable = config['controllable']
    noncontrollable = config['noncontrollable']
    read_params = config['read_params']
    # load model
    model = read_pickle(args.model_path)

    # TODO: think about how to pass args and kwargs in here ...
    data = read_file(args.input_file, args.date_label, *read_params['args'], **read_params['kwargs'])

    if read_params['index_col'] != '':
        data.drop(labels='index', axis=1, inplace=True)
        
    data = data[data['OUTLET'] >= 280]

    max_decrease = [thing[0] for thing in controllable.values()]
    max_increase = [thing[1] for thing in controllable.values()]

    min_bounds = data.loc[:, controllable.keys()] + max_decrease
    max_bounds = data.loc[:, controllable.keys()] + max_increase

    min_bounds.loc[min_bounds['OIL'] < 0, 'OIL'] = 0 
    min_bounds.loc[min_bounds['COMBUSTION_AIR'] < 0, 'COMBUSTION_AIR'] = 0 

    bounds = list(zip(zip(min_bounds['OIL'], max_bounds['OIL']), zip(min_bounds['GAS'], max_bounds['GAS']), zip(min_bounds['COMBUSTION_AIR'], max_bounds['COMBUSTION_AIR']), zip(min_bounds['INLET_TEMP'], max_bounds['INLET_TEMP'])))

    outlet = data.loc[:, 'OUTLET'].copy()

    

    logging.info("Formatting Data for multiprocessing")
    # format data for multiprocessing
    out = mp_optimization(data, args.date_label, controllable.keys(), noncontrollable, bounds, model, args.max_iter, args.n_cores, outlet, args.c_value)

    logging.info(f"Saving results to {args.out_path}")
    # FIXME: clean up this check ...
    no_slash = osp.split(args.out_path)[:-1]
    if no_slash[0] == '':
        no_slash = ('.',)
    out_dir = osp.join(*no_slash)

    if not osp.exists(out_dir):
        logging.info("Output path not detected ... creating path")
        os.makedirs(out_dir)

    out.to_csv(args.out_path)

    run_time = datetime.datetime.now() - start
    logging.info(f"Total run time: {run_time.total_seconds() / 60:.3f} (minutes)")

    return


if __name__ == '__main__':
    main()
