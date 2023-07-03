"""
Monitoring framework action class
"""
import logging
import urllib
from typing import List
import os

from utils import ADDRESS
from utils import FAIL_TASK
from utils import NAME
from utils import PRINT
from utils import SEND_MAIL
from utils import TYPE

LOG = logging.getLogger(__name__)
SUPPORTED_TYPES = [SEND_MAIL, PRINT]


class Action:
    def __init__(self, actions, results):
        self.actions = actions
        self.results = results

    def execute(self, kwargs):
        for action in self.actions:
            if action[TYPE] not in SUPPORTED_TYPES:
                raise Exception(action[NAME] + ' uses a not supported type ' + action[TYPE])
            self.call_action(action, kwargs)
            
    def call_action(self, action, kwargs):
        if action[TYPE] == SEND_MAIL:
            self.call_sendmail_action(action, kwargs)
        else:
            self.call_print_action(action, kwargs)

    def call_print_action(self, action, kwargs):
        body = '{task_id} - {execution_date} \n'.format(**kwargs)

        respected_rules = True
        for key in self.results:
            result = self.results[key]
            respected_rules = respected_rules and result.respected_rule
            body += result.__str__()
            if result.fail_task and not result.respected_rule:
                LOG.info(f'{result.name} failed ')

        print(f'has respected the rules? {respected_rules}')
        print(body)
    
    def call_sendmail_action(self, action, kwargs):
        title = '{task_id} - {execution_date}'.format(**kwargs)
        title = f"DEV {title}" if "dev" in os.environ["COMPOSER_ENVIRONMENT"] else title
        body = ''
        respected_rules = True
        fail_task = False
        for key in self.results:
            result = self.results[key]
            respected_rules = respected_rules and result.respected_rule
            if result.fail_task and not result.respected_rule:
                fail_task = True
            if not result.respected_rule:
                for evaluation in result.evaluations:
                    args = evaluation.get('args')
                    respected_rule = evaluation.get('respected_rule')
                    if not respected_rule:
                        if result.graph:
                            body += result.graph + '\n'
                        body += f"{result.name} {args.get('dimensions') if args.get('dimensions') else ''}\n"
                        body += args.get('message') + '\n\n'

        LOG.info(body)
        # body has content when a rule is violated or when always_send is True
        if body:
            url = f'{os.environ["AIRFLOW__WEBSERVER__BASE_URL"]}/admin/airflow/log?'
            ts = '{ts}'.format(**kwargs)
            ts_parsed = urllib.parse.quote_plus(ts)
            uri = 'task_id={task_id}&dag_id={dag_id}'.format(**kwargs) + f'&execution_date={ts_parsed}&format=json'

            # append a link to task execution
            body = url + uri + '\n' + body
            Action.__send_email("no-reply@gmail.com", action[ADDRESS], title if respected_rules else f'{title} FAIL',
                                body, None)

        if fail_task:
            raise Exception(f'at least one {FAIL_TASK} rule found')

    @staticmethod
    def __send_email(from_addr: str, to_addr: str, subject: str, body: str, attachments: List[str]) -> None:
        raise Exception(f'TODO - needs to be implemented')
