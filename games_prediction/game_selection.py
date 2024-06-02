from abc import ABC, abstractmethod
from typing import List
import polars as pl
from sqlalchemy import func

from steam_players_orm.db_queries import top_n_accum
from steam_players_orm.db_model import GamesData, engine

class GameSelectionStrategy(ABC):
    @abstractmethod
    def select(self, sample:dict, **kwargs):
        pass

class MostPopular(GameSelectionStrategy):
    def __init__(self, n_total_hours=50, n_total_owned=50) -> None:
        self.n_total_hours = n_total_hours
        self.n_total_owned = n_total_owned

    def select(self, games_sample:dict) -> List:
        query_top_played = top_n_accum(
            self.n_total_hours
            )
        df_top_played = pl.read_database(
            query=query_top_played, 
            connection=engine
        )
        query_top_owned = top_n_accum(
            self.n_total_owned,
            column=GamesData.game_code,
            col_label="game_code",
            accum_func=func.count,
            accum_col=GamesData.profile_code,
            accum_label="num_of_players"
            )
        
        df_top_owned = pl.read_database(
            query=query_top_owned, 
            connection=engine
            )
        most_popular_by_ownage_and_playtime = list(
            set(df_top_owned['game_code'].to_list())
            .union(set(df_top_played['game_code'].to_list()))
            .union(set(list(games_sample.keys())))
        )
        return most_popular_by_ownage_and_playtime









# df_top_played = pl.read_database(
#     query=top_n_accum(n_total_hours), connection=engine
# )

# query_top_owned = top_n_accum(
#     n_total_owned,
#     column=GamesData.game_code,
#     col_label="game_code",
#     accum_func=func.count,
#     accum_col=GamesData.profile_code,
#     accum_label="num_of_players")

# df_top_owned = pl.read_database(
#     query=query_top_owned, connection=engine
# )

# popular_games = set(df_top_owned['game_code'].to_list()).union(set(df_top_played['game_code'].to_list()))
# game_codes = list(set(history_dict.keys()).union(popular_games))
# print(f"There are {len(game_codes)-len(history_dict.keys())} more popular games not included in history_dict")