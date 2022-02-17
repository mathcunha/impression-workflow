from argparse import ArgumentParser
from models import Impression, Revenue
import logging
import threading
import tasks
import pandasql
import json

FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(format=FORMAT)
logging.root.setLevel(logging.INFO)


def read_impressions(files):
    logging.info(files)
    for file_name in files:
        tasks.process_impression_file(file_name)
    
    logging.info('deduping impressions')
    tasks.process_deduped_impressions()


def read_clicks(files):
    logging.info(files)
    for file_name in files:
        tasks.process_click_file(file_name)


def conversions():
    deduped_impression_df = tasks.load_deduped_impressions()
    click_df = tasks.load_clicks()
    tasks.attribute_impressions(deduped_impression_df, click_df)
    return tasks.load_impressions()


def calculate_metrics(impressions_df):
    with open("resources/calculate_metrics.ql", "r") as input:
        query = input.read()

    output_df = pandasql.sqldf(query, locals())
    output_impressions = [(Impression(row.app_id, row.country_code, row.impressions, row.clicks, row.revenue)).__dict__ for index, row in output_df.iterrows() ]
    with open('output_app_id_country_metrics.json', 'w') as output:
        output.write(json.dumps(output_impressions))


def recommend_advertisers(impressions_df):
    with open("resources/recommend_ad.ql", "r") as input:
        query = input.read()

    top_5_df = pandasql.sqldf(query, locals())
    output_revenue = [(Revenue(row.app_id, row.country_code, row.recommended_advertiser_ids)).__dict__ for index, row in top_5_df.iterrows() ]

    with open('output_app_id_country_top_advertisers.json', 'w') as output:
        output.write(json.dumps(output_revenue))


class Workflow:
    def __init__(self, impressions, clicks):
        self.impressions = impressions
        self.clicks = clicks

    def start(self):
        # creating thread
        impressions = threading.Thread(target=read_impressions, args=(self.impressions, ))
        clicks = threading.Thread(target=read_clicks, args=(self.clicks, ))
    
        impressions.start()
        clicks.start()
    
        impressions.join()
        clicks.join()

        impressions_df = conversions()
        calculate_metrics(impressions_df)
        recommend_advertisers(impressions_df)
        


parser = ArgumentParser(description="Impression Workflow")
parser.add_argument("-i", dest="impressions", required=True, help="the list of impression files", nargs='+')
parser.add_argument("-c", dest="clicks", required=True, help="the list of click files", nargs='+')


if __name__ == "__main__":
    options = parser.parse_args()
    workflow = Workflow(options.impressions, options.clicks)
    workflow.start()
