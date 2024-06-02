from abc import ABC, abstractmethod
import polars as pl

from steam_players_orm.db_model import engine
from steam_players_orm.db_queries import distribution_distance_query


class TraningUsersStrategy(ABC):
    @abstractmethod
    def select_users(self, games_sample:dict, **kwargs):
        pass


class CloseInL1(TraningUsersStrategy):
    def __init__(self, input_total=None, num_of_results=None):
        self.input_total = input_total
        self.num_of_results = num_of_results

    def select_users(self, games_sample:dict):
        profiles_close_to_user = pl.read_database(
            query=distribution_distance_query(
                input_dictionary = games_sample, 
                input_total=self.input_total, 
                num_of_results=self.num_of_results), 
            connection=engine.connect()
        )
        return profiles_close_to_user
