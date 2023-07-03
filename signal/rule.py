"""
Monitor Rule class
SUPPORTED_TYPES:
    The rules can be applied to observations of a single metric/measure at different time intervals, TIME_SERIES
    or the rules can be applied to several metrics/measures at the same point in time, CROSS_SECTION
FUNCTIONS:
    The functions available at the moment are:
        IN - Check if the metric value belong to a list of known values
        GT - Check if a metric value is greater than another metric value. METRIC_A > METRIC_B
        GTE - greater than equal
        ST - smaller than
        STE - smaller than equal
        RGT - relative greater than. abs(METRIC_A - METRIC_B) / METRIC_A > THRESHOLD
        RGTE - relative greater than equal
        RST - relative smaller than
        RSTE - relative smaller than equal
    The funtions return True when the rule is RESPECTED.
"""

import logging

import pandas as pd

from utils import *
from statistic_rule import StatisticRule
from rule_result import RuleResult

LOG = logging.getLogger(__name__)
SUPPORTED_TYPES = [TIME_SERIES, CROSS_SECTION, STATS_SERIES]
FUNCTIONS = [RGT, GT, IN, ST, STE, GTE, RST, RSTE, RGTE, STD_METHOD]


class Rule:
    def __init__(self, rules, fetcher_map, ack_dt):
        self.rules = rules
        self.fetcher_map = fetcher_map
        self.rules_map = {}
        self.ack_dt = ack_dt

    def execute(self):
        for rule in self.rules:
            value = self.rules_map.get(rule[NAME], None)
            if value:
                raise Exception(rule[NAME] + ' already in use')
            if rule[TYPE] not in SUPPORTED_TYPES:
                raise Exception(rule[NAME] + ' uses a not supported type ' + rule[TYPE])
            if rule[SPEC][METRIC] is None:
                raise Exception(rule[NAME] + ' needs to provide spec.metric property')
            if rule[SPEC][FUNCTION] not in FUNCTIONS:
                raise Exception(rule[NAME] + ' uses a not supported function ' + rule[SPEC][FUNCTION])
            self.rules_map[rule[NAME]] = rule[NAME]

        for rule in self.rules:
            self.execute_rule(rule)

        return self.rules_map

    def apply_stats_series(self, rule):
        split_metric = rule[SPEC][METRIC].split('.')
        fetcher_df = self.fetcher_map[split_metric[0]]
        dimensions = rule[SPEC].get(DIMENSIONS, None)
        LOG.info(dimensions)
        if rule[SPEC][FUNCTION] == STD_METHOD:
            result = StatisticRule.execute(rule, split_metric[1], dimensions, fetcher_df, rule[SPEC][SCALAR],
                                         StatisticRule.apply_std_method, rule[SPEC].get(BY_PASS_UPPER_LIMIT, False))
            self.rules_map[rule[NAME]] = result
        else:
            raise Exception(rule[NAME] + ' uses a not implemented function ' + rule[SPEC][FUNCTION])
    
    def apply_time_series(self, rule):
        split_metric = rule[SPEC][METRIC].split('.')
        fetcher_df = self.fetcher_map[split_metric[0]]
        dt_column_name = fetcher_df.columns[0]
        dates = fetcher_df.iloc[:, 0].unique()
        dates = sorted(dates, reverse=True)
        del self.rules_map[rule[NAME]]

        for i in range(1, len(dates)):
            current_dt = dates[0]
            past_dt = dates[i]
            if str(current_dt) in self.ack_dt or str(past_dt) in self.ack_dt:
                LOG.info(f'skipping rule for dates ({past_dt} and {current_dt})')
                continue

            current_df = fetcher_df[fetcher_df[dt_column_name] == current_dt]
            past_df = fetcher_df[fetcher_df[dt_column_name] == past_dt]

            dimensions = rule[SPEC].get(DIMENSIONS, None)
            LOG.info(dimensions)
            if dimensions:
                merged_df = current_df.merge(right=past_df, how="outer", on=dimensions)
            else:
                # cross is not available at the current pandas versions
                # merged_df = current_df.merge(right=past_df, how="cross")
                left = current_df.assign(key=1)
                right = past_df.assign(key=1)
                merged_df = left.merge(right, how="outer", on='key')

            args = [merged_df[f'{split_metric[1]}_x'], merged_df[f'{split_metric[1]}_y']]
            # the difference in days between current and past dates increases the threshold at a THRESHOLD_DELTA rate,
            # default rate is 0.005, but you can override this by setting THRESHOLD_DELTA at the config file.
            days = abs((current_dt - past_dt).days)
            threshold = rule[SPEC][THRESHOLD] + rule[SPEC].get(THRESHOLD_DELTA, 0.005) * (days - 1)

            message = f"{current_dt} {{x}}\n{past_dt} {{y}}\n"
            result = Rule.call_function(rule, args, message=message, threshold=threshold)
            if dimensions:
                result.set_dimension_values(dimensions, merged_df)
            self.rules_map[f'{rule[NAME]}_{past_dt}'] = result
        
    def apply_cross_section(self, rule):
        metrics = []
        if type(rule[SPEC][METRIC]) is str or type(rule[SPEC][METRIC]) == int or type(rule[SPEC][METRIC]) == float:
            metrics.append(rule[SPEC][METRIC])
        else:
            metrics.extend(rule[SPEC][METRIC])
        args = []

        for metric in metrics:
            if type(metric) is str:
                split_metric = metric.split('.')
                if len(split_metric) == 1:
                    args.append(metric)
                else:
                    fetcher_df = self.fetcher_map[split_metric[0]]
                    column = fetcher_df[split_metric[1]]
                    args.append(column)
            else:
                args.append(metric)

        result = Rule.call_function(rule, args, message='', threshold=rule[SPEC].get(THRESHOLD))
        self.rules_map[rule[NAME]] = result
        
    def execute_rule(self, rule):
        if rule[TYPE] == TIME_SERIES:
            self.apply_time_series(rule)
        elif rule[TYPE] == STATS_SERIES:
            self.apply_stats_series(rule)
        else:
            self.apply_cross_section(rule)

    @staticmethod
    def call_function(rule, metrics, **kwargs):
        if rule[SPEC][FUNCTION] == IN:
            return Rule.call_in_function(rule, metrics)
        elif rule[SPEC][FUNCTION] in [GT, ST, GTE, STE]:
            return Rule.call_absolute_function(rule[SPEC][FUNCTION], rule, metrics)
        elif rule[SPEC][FUNCTION] in [RGT, RST, RGTE, RSTE]:
            return Rule.call_relative_function(rule[SPEC][FUNCTION], rule, metrics, kwargs["message"], kwargs["threshold"])
        raise Exception(rule[NAME] + ' uses a not implemented function ' + rule[SPEC][FUNCTION])

    @staticmethod
    def call_in_function(rule, args):
        result = RuleResult(rule)
        items = rule[SPEC][LIST]
        for row in args[0]:
            output = row in items
            result.add_item({'os': row, 'message': {row} if output else f'new entry found: {row}'}, output)
        return result

    @staticmethod
    def _load_bifunction_series(rule, args):
        """
        bifunction represents a function that accepts two arguments and produces a result. Like
        METRIC_A > METRIC_B. It accepts two metrics and returns a boolean.
        This method makes sure that both args passed to the function will have the same length. If the args are both
        pandas.Series, the length if guaranteed by the dataframe merge. Otherwise, the function identifies
        the one arg of type pandas.Series and then creates an array with the other value presented at the other arg.
        :param rule:
        :param args:
        :return:
        """
        series_x = None
        series_y = None
        lit = None
        index = -1
        # Check if both args are pandas series
        if type(args[0]) is pd.Series and type(args[1]) is pd.Series:
            series_x = args[0]
            series_y = args[1]
        # Check if args[0] is pandas series
        elif type(args[0]) is pd.Series:
            series_x = args[0]
            index = series_x.shape[0]
            lit = args[1]
        # Check if args[1] is pandas series
        elif type(args[1]) is pd.Series:
            series_y = args[1]
            index = series_y.shape[0]
            lit = args[0]
        else:
            raise Exception(f'{rule[NAME]} is a {rule[SPEC][FUNCTION]} and expects at least one Series metric ' + args)

        if index != -1:
            series = []
            for i in range(index):
                series.append(lit)
            if series_x is None:
                series_x = series
            else:
                series_y = series

        LOG.info(f"series_x: {series_x}")
        LOG.info(f"series_y: {series_y}")
        return series_x, series_y

    @staticmethod
    def call_absolute_function(function, rule, args):
        result = RuleResult(rule)
        (series_x, series_y) = Rule._load_bifunction_series(rule, args)

        operator = ''
        if function == GT:
            operator = '>'
            def absolute_function(a, b): return a > b
        elif function == ST:
            operator = '<'
            def absolute_function(a, b): return a < b

        for x, y in zip(series_x, series_y):
            output = absolute_function(x, y)
            result.add_item({'x': x, 'operator': operator, 'y': y, 'message':f'{x} {operator} {y} is {output}'}, output)

        return result

    @staticmethod
    def call_relative_function(function, rule, metrics, message, threshold):
        result = RuleResult(rule)
        (series_x, series_y) = Rule._load_bifunction_series(rule, metrics)
        min_abs_diff = rule[SPEC].get(MIN_ABS_DIFF, 3)

        operator = ''
        if function == RGT:
            operator = '>'
            def has_metric_respected_threshold(value): return value > threshold
        elif function == RST:
            operator = '<='
            def has_metric_respected_threshold(value): return value <= threshold

        for x, y in zip(series_x, series_y):
            if pd.isna(y):
                result.add_item({'skipped': 'has nan metric'}, True)
            else:
                if abs(x - y) <= min_abs_diff:
                    result.add_item({'skipped': f'metrics diff, {abs(x - y)} <= {min_abs_diff}'}, True)
                else:
                    try:
                        evaluation = round((abs(x - y) / max(x, y)), 2)
                        output = has_metric_respected_threshold(evaluation)
                    except ZeroDivisionError:
                        output = y == 0
                    rule_output_message = 'respects' if output else 'violates'
                    result.add_item({'x': x, 'y': y, 'message': f'{message.format(x=x, y=y)}abs({x} - {y}) / {max(x, y)}'
                                                                f'\n{evaluation} {rule_output_message} expectation of '
                                                                f'{operator} {threshold}'}, output)

        return result
