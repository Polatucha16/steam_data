from abc import ABC, abstractmethod
import polars as pl

from steam_players_orm.polars_funcs import map_history_to_ranks_from_percentiles
from steam_players_orm.db_model import engine
from steam_players_orm.db_queries import (
    hour_percentiles_for_games, 
    players_history_in_games
    )

class TraningDataStrategy(ABC):
    # @abstractmethod
    # def prepare(self, user_code, games_sample, traning_users, selected_games, **kwargs)->tuple:
    #     """prepare both traning_data and the user_sample"""
    #     pass
    @abstractmethod
    def data_for_users(self, profile_codes, game_codes, **kwargs)->pl.DataFrame:
        """ get histories of users in games and transform """
        pass

    @abstractmethod
    def data_for_sample(self, user_code, game_codes, **kwargs):
        """ transform the knowladge about user in sample"""
        pass
    # def prepare_sample(self):
    #     pass
    # @abstractmethod
    # def transform(self, profile_ids, selected_games):
    #     pass


class QuantileRanking(TraningDataStrategy):

    def __init__(self, percentiles=[0.2, 0.4, 0.6, 0.8]) -> None:
        self.percentiles = percentiles
    
    def get_percentiles_for(self, game_codes):
        self.df_hour_percentiles = pl.read_database(
            query=hour_percentiles_for_games(
                game_codes_list=game_codes, 
                q_list=self.percentiles
            ),
            connection=engine,
        )

    def data_for_users(self, profile_codes: list, game_codes: list):
        """ Make sure that (profile_codes is list) and (that adviced user is not in it)."""

        if not hasattr(self, 'df_hour_percentiles'):
            self.get_percentiles_for(game_codes)
        
        self.history_for_users: pl.DataFrame
        self.history_for_users = pl.read_database(
            query=players_history_in_games(
                game_codes=game_codes,
                profile_codes=profile_codes,
            ),
            connection=engine,
        )

        ranks_for_users = map_history_to_ranks_from_percentiles(
            self.history_for_users, self.df_hour_percentiles
        )

        return ranks_for_users
    
    def data_for_sample(self, user_code:int, games_sample:dict):

        if not hasattr(self, 'df_hour_percentiles'):
            self.get_percentiles_for(list(games_sample.keys()))

        temp_dict = {'profile_code': [user_code]}
        temp_dict.update({str(key):[val] for key, val in games_sample.items()})
        df_hisotry_for_sample = pl.from_dict(temp_dict)

        ranks_for_sample = map_history_to_ranks_from_percentiles(
            df_hisotry_for_sample, self.df_hour_percentiles
        )

        return ranks_for_sample
