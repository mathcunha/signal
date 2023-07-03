from utils import *

class RuleResult:
    def __init__(self, rule):
        self.name = rule[NAME]
        self.type = rule[SPEC][FUNCTION]
        self.respected_rule = True
        self.fail_task = rule.get(FAIL_TASK, False)
        self.graph = rule.get(GRAPH, None)
        self.evaluations = []

    def add_item(self, args, respected_rule):
        self.respected_rule = self.respected_rule and respected_rule
        self.evaluations.append({'args': args, 'respected_rule': respected_rule})

    def __str__(self) -> str:
        report = f'\n{self.type} is {self.respected_rule} for {self.name}\n'
        for evaluation in self.evaluations:
            args = evaluation.get('args')
            respected_rule = evaluation.get('respected_rule')
            report += f'args {args} is {respected_rule} \n'
        return report

    def set_dimension_values(self, dimensions, df):
        for idx, evaluation in enumerate(self.evaluations):
            row = df.iloc[[idx]]
            args = evaluation.get('args')
            args['dimensions'] = {}
            for dimension in dimensions:
                args['dimensions'][dimension] = row[dimension].iloc[0]