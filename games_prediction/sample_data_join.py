from abc import ABC, abstractmethod
import polars as pl
import numpy as np


class JoiningStrategy(ABC):
    @abstractmethod
    def join(self, user_data, traning_data, **kwargs)->pl.DataFrame:
        pass
    def extract(self, M_filled)->np.ndarray:
        pass


class LastColumn(JoiningStrategy):
    def join(self, user_data: pl.DataFrame, traning_data: pl.DataFrame):
        df_concat = pl.concat([traning_data, user_data], how="diagonal")
        numpy_row_users = df_concat.select(pl.exclude("profile_code")).to_numpy().astype(float)
        numpy_col_users = numpy_row_users.T
        return numpy_col_users
    
    def extract(self, M_filled:np.ndarray):
        return M_filled[:,-1]