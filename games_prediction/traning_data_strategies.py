from abc import ABC, abstractmethod
import polars as pl

from steam_players_orm.polars_funcs import map_history_to_ranks_from_percentiles
from steam_players_orm.db_model import engine
from steam_players_orm.db_queries import (
    hour_percentiles_for_games, 
    players_history_in_games
    )

class TraningDataStrategy(ABC):
    @abstractmethod
    def prepare(self, user_code, games_sample, traning_users, selected_games, **kwargs)->tuple:
        """prepare both traning_data and the user_sample"""
        pass


class QuantileRanking(TraningDataStrategy):

    def __init__(self, percentiles=[0.2, 0.4, 0.6, 0.8]) -> None:
        self.percentiles = percentiles

    def prepare(self, user_id: int, games_sample:dict, traning_users: pl.DataFrame, selected_games: list):

        df_hour_percentiles = pl.read_database(
            query=hour_percentiles_for_games(
                game_codes_list=selected_games, 
                q_list=self.percentiles
            ),
            connection=engine,
        )

        traning_users_ids = traning_users["profile_code"].to_list()
        traning_users_ids = [id for id in traning_users_ids if id != user_id]

        traning_users_history: pl.DataFrame
        traning_users_history = pl.read_database(
            query=players_history_in_games(
                game_codes=selected_games,
                profile_codes=traning_users_ids,
            ),
            connection=engine,
        )

        traning_users_ranks = map_history_to_ranks_from_percentiles(
            traning_users_history, df_hour_percentiles
        )

        temp_dict = {'profile_code': [user_id]}
        temp_dict.update({str(key):[val] for key, val in games_sample.items()})
        df_user_sample = pl.from_dict(temp_dict)

        user_ranks = map_history_to_ranks_from_percentiles(
            df_user_sample, df_hour_percentiles
        )

        return traning_users_ranks, user_ranks
