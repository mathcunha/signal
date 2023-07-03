import logging
import json
import pandas as pd
from action import Action
from fetcher import Fetcher
from rule import Rule
from utils import ACTIONS
from utils import FETCHERS
from utils import RULES
from io import BytesIO

LOG = logging.getLogger(__name__)
GS_PREFIX = "gs://"


class Monitor:
    """
    Executes a monitor config
    """

    def __init__(self, config, project_id, kwargs):
        if type(config) is str:
            self.config = Monitor.read_config_file(config)
        else: 
            self.config = config
        self.project = project_id
        self.kwargs = kwargs

    def run(self):
        fetcher = Fetcher(self.config[FETCHERS], self.project)
        fetcher_map = fetcher.execute(self.kwargs)
        LOG.info(fetcher_map)
        ack_dt = Monitor.get_ack_dt(self.kwargs['task_id'])
        ruler = Rule(self.config[RULES], fetcher_map, ack_dt)
        rules_map = ruler.execute()
        LOG.info(rules_map)
        action = Action(self.config[ACTIONS], rules_map)
        action.execute(self.kwargs)

    @staticmethod
    def get_ack_dt(task_id):
        # TODO - create new config
        ack_path = 'gs://ack_list.csv'
        body = Monitor.read_gs_file(ack_path)
        buffer = BytesIO(body)
        ack_df = pd.read_csv(buffer)
        ack_dt = ack_df[ack_df.task.isin(['*', task_id])]['dt']
        LOG.info(f'acknowledged dates {ack_dt}')
        return ack_dt.tolist()

    @staticmethod
    def execute(config, project_id, **kwargs):
        monitor = Monitor(config, project_id, kwargs)
        monitor.run()

    @staticmethod
    def read_gs_file(file):
        LOG.info(f'config URL {file}')
        bucket_name = file.split('/')[2]
        index = len(GS_PREFIX) + 1 + len(bucket_name)
        file_name = file[index:]
        LOG.info(f'loading {file_name} from bucket {bucket_name}')

        from google.cloud import storage
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blog = bucket.get_blob(file_name)
        return blog.download_as_string()

    @staticmethod
    def read_gs_config_file(file):
        body = Monitor.read_gs_file(file)
        dictionary = json.loads(body)
        return dictionary

    @staticmethod
    def read_config_file(file):
        if file.startswith(GS_PREFIX):
            return Monitor.read_gs_config_file(file)

        with open(file, "r") as input:
            config = input.read()
            dictionary = json.loads(config)
            return dictionary
