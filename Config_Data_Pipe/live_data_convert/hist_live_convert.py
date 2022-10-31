import argparse
import logging
import pandas as pd

from utils import read_json, write_json
import data_mapper
from data_mapper import conversion
import utils

def parse_args() -> argparse.Namespace:
    """Function to parse command line arguments"""

    parser = argparse.ArgumentParser()

    parser.add_argument('hist_data_path', type=str, help='Path to info.json')
    parser.add_argument('info_path', type=str, help='Path to info.json')
    parser.add_argument('mapper_path', type=str, help='Path to Mapper')
    parser.add_argument('config_path', type=str, help='Path to config.json')
    parser.add_argument('--output_path', type=str, required = False, help='Path to save output', default = "./Output")
    
    args = parser.parse_args()

    return args


def main():

    args = parse_args()

    formatstr = '%(asctime)s: %(levelname)s: %(funcName)s Line: %(lineno)d %(message)s'
    datestr = '%m/%d/%Y %H:%M:%S'
    logging.basicConfig(
        level=logging.INFO, 
        format=formatstr, 
        datefmt=datestr, 
        handlers=[
            logging.FileHandler('config_update.log'),
            logging.StreamHandler()
            ]
        )

    # load in data
    logging.info("Loading files")
    hist_df = pd.read_csv(args.hist_data_path)
    info = utils.read_json(args.info_path)
    config = read_json(args.config_path)
    mapper = pd.read_excel(args.mapper_path, config['mapper_sheet'])


    mh = data_mapper.MapperHandler(*config['data_mapper_columns'])

    live_format, sub, name_to_id, id_to_name = conversion.convert_to_ld(hist_df, mapper, mh, config['property_ids'])
    live_format['Date'] = pd.to_datetime(live_format['Date'])
    live_format = live_format.set_index('Date')

    all_object_ids = [*info.values()]
    missing_tags = [91453]
    all_object_names = ['___'.join(id_to_name[object_id, -6]) for object_id in all_object_ids if object_id not in missing_tags]

    for i in range(5):
            data_mapper.put_data(live_format.loc[:, all_object_names].iloc[[i], :], args.output_path, name_to_id)
    
    # data_mapper.put_data(live_format.loc[1:3, all_object_names].iloc[[-3], :], './', name_to_id)

    return

if __name__ == '__main__':
    main()
