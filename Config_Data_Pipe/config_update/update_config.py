import argparse
import logging
import pandas as pd
import numpy as np

from utils import read_json, write_json, short_to_long_and_missing_tags, delete_duplicate_tags, check_missing_tags
from utils import TagMissingInMapper
import conversion

def parse_args() -> argparse.Namespace:
    """Function to parse command line arguments"""

    parser = argparse.ArgumentParser()

    parser.add_argument('mapper_path', type = str, help = "Path to mapper (loading)")
    parser.add_argument('tag_list_path', type=str, help='Path to tag list (loading)')
    parser.add_argument('config_path', type=str, help='Path to config json (loading)')
    parser.add_argument('--info_path', type=str, required = False, help='Path to save info.json to (saving)', default = "./Output/info.json")
    parser.add_argument('--filtered_mapper_path', type=str, required = False, help='Path to save filtered mapper to (saving)', default = "./Output/filtered_mapper.csv")
    parser.add_argument('--names_path', type=str, required = False, help='Path to save name.json to (saving)', default = "./Output/name.json")

    args = parser.parse_args()

    return args

def main():
    """Main Function"""
    # setup logger
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

    args = parse_args()

    # Load in configs, mapper, tags, and tag info
    logging.info("Reading Files")
    config = read_json(args.config_path)
    tags_df = pd.read_csv(args.tag_list_path)
    mapper = pd.read_excel(args.mapper_path, config['mapper_sheet'])


    # add additional variables to original variables
    all_tag_df = pd.DataFrame({'Tags':[*tags_df['Tag']]}) 

    # drop duplicates
    slim_df = delete_duplicate_tags(all_tag_df)

    # match shortened names to long names from mapper, return long name
    new_tags, missing_tags = short_to_long_and_missing_tags(slim_df, mapper)

    # save tags not in mapper
    write_json(missing_tags, './Output/tags_not_in_mapper.json')

    # get live id's, zip with historical names
    live_tags = conversion.get_ids_from_names(mapper, new_tags)
    hist_live_tags = dict(zip(new_tags, live_tags))

    # save historical names and mapper names
    hist_original_names = dict(zip([tag for tag in all_tag_df['Tags'] if tag not in missing_tags] , new_tags))
    hist_original_names.update(dict.fromkeys([tag for tag in missing_tags], "not in mapper")) 
    write_json(hist_original_names, args.names_path)

    # saving info.json
    logging.info("Saving info.json")
    write_json(hist_live_tags, args.info_path)

    # Subsetting and saving mapper
    filtered_map = mapper[mapper['Datapoint Name'].isin(new_tags)].drop_duplicates()
    logging.info("Saving Mapper")
    filtered_map.to_csv(args.filtered_mapper_path)

    # print out missing tags
    try:
        check_missing_tags(slim_df, mapper)
    except(TagMissingInMapper) as e:
        logging.warning(str(e))

    return

if __name__ == '__main__':
    main()   
