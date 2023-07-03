from rule_result import RuleResult
import logging

LOG = logging.getLogger(__name__)

class StatisticRule:

    @staticmethod
    def execute(rule, metric_column, dimensions, fetcher_df, scalar, method, bypass_upper_limit = True):
        dt_column_name = fetcher_df.columns[0]
        dates = fetcher_df.iloc[:, 0].unique()
        dates = sorted(dates, reverse=True)
        result = RuleResult(rule)

        execution_date = dates[0]
        if dimensions:
            unique_dimensions_df = fetcher_df[dimensions].groupby(dimensions).count().reset_index()
            for i in range (unique_dimensions_df.shape[0]):
                df_by_dimension = None
                first_dimension_filter = True
                dimensions_values = {}
                for dimension in dimensions:
                    dimensions_values[dimension] = unique_dimensions_df.iloc[i][dimension]
                    LOG.info(f"filtering by {dimension} equal to {dimensions_values[dimension]}")
                    if first_dimension_filter:
                        df_by_dimension = fetcher_df[fetcher_df[dimension] == dimensions_values[dimension]]
                        first_dimension_filter = False
                    else:
                        df_by_dimension = df_by_dimension[df_by_dimension[dimension] == dimensions_values[dimension]]
                LOG.info(df_by_dimension.shape)

                # Define the filtering condition for the current value based on the `dt_column_name` and `execution_date` parameters.
                current_filter_condition = df_by_dimension[dt_column_name] == execution_date

                # Filter the `df_by_dimension` DataFrame to include only rows that match the `current_filter_condition`.
                current_rows_df = df_by_dimension[current_filter_condition]

                # Extract the `metric_column` values from the matching rows.
                current_values_df = current_rows_df[metric_column]

                # Select the first (and presumably only) value in the `current_values` Series, and assign it to `current_value`.
                current_value = current_values_df.iloc[0]

                # Define the filtering condition for the past values based on the `dt_column_name` and `execution_date` parameters.
                past_filter_condition = df_by_dimension[dt_column_name] != execution_date

                # Filter the `df_by_dimension` DataFrame to include only rows that match the `past_filter_condition`.
                past_rows_df = df_by_dimension[past_filter_condition]

                # Select the `metric_column` values from the matching rows, and assign them to the `past_values_df` variable.
                past_values_df = past_rows_df[metric_column]

                method(current_value,
                       past_values_df,
                       scalar, result, dimensions_values, bypass_upper_limit)
        else:
            # Define the filtering condition for the current value based on the `dt_column_name` and `execution_date` parameters.
            current_filter_condition = fetcher_df[dt_column_name] == execution_date

            # Filter the `fetcher_df` DataFrame to include only rows that match the `current_filter_condition`.
            current_rows_df = fetcher_df[current_filter_condition]

            # Extract the `metric_column` values from the matching rows.
            current_values_df = current_rows_df[metric_column]

            # Select the first (and presumably only) value in the `current_values` Series, and assign it to `current_value`.
            current_value = current_values_df.iloc[0]

            # Define the filtering condition for the past values based on the `dt_column_name` and `execution_date` parameters.
            past_filter_condition = fetcher_df[dt_column_name] != execution_date

            # Filter the `fetcher_df` DataFrame to include only rows that match the `past_filter_condition`.
            past_rows_df = fetcher_df[past_filter_condition]

            # Select the `metric_column` values from the matching rows, and assign them to the `past_values_df` variable.
            past_values_df = past_rows_df[metric_column]
            method(current_value,
                   past_values_df,
                   scalar, result, bypass_upper_limit=bypass_upper_limit)
        return result

    @staticmethod
    def apply_std_method(metric_value, df, scalar, result, dimensions = '', bypass_upper_limit = True):
        # Compute summary statistics of the metric values in `df`, such as count, mean, and standard deviation.
        # See https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.describe.html for more info.
        stats = df.describe()
        mean = stats[1]
        std = stats[2]
        min_value = max(mean - (std * scalar), stats[3])
        max_value = mean + (std * scalar)
        output = metric_value >=  min_value and (bypass_upper_limit or metric_value <= max_value)
        LOG.info(f'{output} for metric_value:{metric_value} between min:{min_value} and (max:{max_value} or ({bypass_upper_limit})) with std:{std}')

        if not output:
            if bypass_upper_limit:
                LOG.info(f"is {metric_value} smaller than {min_value:.2f}")
            else:
                LOG.info(f"is {metric_value} smaller than {min_value:.2f} and bigger than {max_value:.2f}")
        rule_output_message = 'inside' if output else 'outside'
        result.add_item({'x': metric_value, 'dimensions': dimensions,
                         'message': f'{metric_value} {rule_output_message} the range [{min_value}, {max_value}]\n'
                                    f'The std is {std}'}, output)
        return output