from abc import ABC, abstractmethod
import numpy as np
import polars as pl

class TestStrategy(ABC):
    @abstractmethod
    def test(self, result:pl.DataFrame, true_values:pl.DataFrame):
        pass

class StatisticalTests(TestStrategy):

    def test(self, result, true_values):
        test_result = {}

        # two_samples = np.vstack((result.to_numpy(), true_values.to_numpy()))
        
        df = pl.concat(
            [true_values.select(pl.all().cast(pl.Float64)), result]
        ).transpose(
            include_header=True,
            header_name="game_code",
            column_names=["true_history", "predictions"],
        )

        true__positives = df.filter((pl.col('true_history') >3.5) & (pl.col('predictions') >= 3.5)).shape[0]
        false_positives = df.filter((pl.col('true_history') <  4) & (pl.col('predictions') >= 3.5)).shape[0]
        false_negatives = df.filter((pl.col('true_history') == 4) & (pl.col('predictions')  < 3.5)).shape[0]
        true__negatives = df.filter((pl.col('true_history')  < 4) & (pl.col('predictions')  < 3.5)).shape[0]

        test_result['Precision-at-highest-threshold'] = true__positives/(true__positives + false_positives)
        test_result['Sensitivity-at-highest-threshold'] = true__positives/(true__positives + false_negatives)
        test_result['Specificity-at-highest-threshold'] = true__negatives/(false_positives + true__negatives)
        return test_result