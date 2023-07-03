"""
Data fetcher class
"""

import logging

from google.cloud import bigquery

from utils import BQ_DATA_FETCHER
from utils import INTERVALS
from utils import NAME
from utils import QUERY
from utils import TYPE

LOG = logging.getLogger(__name__)
SUPPORTED_TYPES = [BQ_DATA_FETCHER]


class Fetcher:

    def __init__(self, fetchers, project):
        self.fetchers = fetchers
        self.project = project

    def execute(self, kwargs):
        fetchers_map = {}
        client = bigquery.Client(self.project)

        for fetcher in self.fetchers:
            value = fetchers_map.get(fetcher[NAME], None)
            if value:
                raise Exception(fetcher[NAME] + ' already in use')
            if fetcher[TYPE] not in SUPPORTED_TYPES:
                raise Exception(fetcher[NAME] + ' uses a not supported type ' + fetcher[TYPE])
            fetchers_map[fetcher[NAME]] = fetcher[NAME]

        for fetcher in self.fetchers:
            LOG.info('Fetching ' + fetcher[NAME] + ' data')
            if BQ_DATA_FETCHER == fetcher[TYPE]:
                df = Fetcher.execute_bq_fetcher(fetcher, client, kwargs)
                fetchers_map[fetcher[NAME]] = df

        return fetchers_map

    @staticmethod
    def execute_bq_fetcher(fetcher, client, kwargs):
        sqls = []
        # sql to fetch current date
        sql = fetcher[QUERY].format(**kwargs)
        sqls.append(sql)

        # sql to fetch intervals and merge results to current
        intervals = fetcher.get(INTERVALS, [])
        for interval in intervals:
            date_clause = f"DATE_SUB(DATE {kwargs['execution_date']}, INTERVAL {interval})"
            sql = fetcher[QUERY].format(execution_date=date_clause).format(**kwargs)
            sqls.append(sql)

        sql = " UNION ALL ".join(sqls)
        LOG.info(sql)
        df = client.query(sql).to_dataframe()

        return df
