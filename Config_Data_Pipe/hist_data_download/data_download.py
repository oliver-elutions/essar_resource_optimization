import argparse
import logging
import pandas as pd

# our package
import SQLTool

from utils import read_json, check_low_counts, LowCountsFullData, NoCountsFullData

def parse_args() -> argparse.Namespace:
    """Function to parse command line arguments"""

    parser = argparse.ArgumentParser()

    parser.add_argument('info_path', type=str, help='Path to info.json (loading)')
    parser.add_argument('config_path', type=str, help='Path to config.json (loading)')
    parser.add_argument('--hist_data_path', type=str, required = False, help='Path to save historical data (saving)', default = "Output/hist_data.csv")
    parser.add_argument('--missing_tag_path', type=str, required = False, help='Path to save missing tags (saving)', default = "Output/tags_not_in_SQL.csv")
    
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
    info = read_json(args.info_path)
    config = read_json(args.config_path)

    # scrape data
    logging.info("Scraping data")
    hist_data_df, missing_tags = SQLTool.download_and_format(
        info.keys(),
        config['client'],
        config['server'], 
        -6,
        start_date =  config['start_date'],
        end_date =  config['end_date'],
        rate = config['rate'],
        unit = config['unit'])

    # save data
    logging.info("Saving data")
    hist_data_df.to_csv(args.hist_data_path)

    # save missing tags
    missing_df = pd.DataFrame({'missing_tags': missing_tags})
    missing_df.to_csv(args.missing_tag_path, index=False)

    check_low_counts(hist_data_df)

    return

if __name__ == '__main__':
    main()