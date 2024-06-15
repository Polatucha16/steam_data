#### Polatucha Recommend 2024

from dataclasses import dataclass, field
from typing import Any, List
import polars as pl

from sqlalchemy import func

from steam_players_orm.db_queries import (
    player_history, 
    distribution_distance_query, 
    top_n_accum, 
    hour_percentiles_for_games,
    players_history_in_games)
from steam_players_orm.db_model import GamesData, engine
from steam_players_orm.polars_funcs import df_to_id_val_dict

from steam_players_orm.polars_funcs import map_history_to_ranks_from_percentiles


@dataclass
class SteamUser:
    """ Either just input a profile number:
        1)
            user = SteamUser(2584514)
        2)
            user = SteamUser(history_fetch=False,history_dict={game_0 : hours_0, ...})
            user.find_close_users()

        to obtain a list of users statistically close to user.history_dict saved to 
            user.df_user_ids_close_to_user
    """
    user_id: int
    history_fetch: bool = True
    history_dict:dict = field(default_factory=dict)
    top_user_games:int = 10

    def __post_init__(self):
        if self.history_fetch:
            self.df_user_history = pl.read_database(
                query=player_history(self.user_id, self.top_user_games),
                connection=engine.connect()
                )
            self.history_dict = df_to_id_val_dict(self.df_user_history, self.top_user_games)
    
    def get_history(self):
        self.__post_init__()
    
    def find_close_users(self, **kwargs):
        self.df_user_ids_close_to_user = pl.read_database(
            query=distribution_distance_query(self.history_dict, **kwargs), 
            connection=engine.connect()
        )


@dataclass
class CloseUsers:
    user:SteamUser
    # history_dict: dict ={}
    players_sample_size:int = 200
    def __post_init__(self):
        self.history_dict = self.user.history_dict.copy()
        self.user_game_codes = list(self.history_dict.keys())
        self.game_hours = list(self.history_dict.values())

    def join_more_games(self, n_total_hours=50, n_total_owned=50):

        df_top_played = pl.read_database(
            query=top_n_accum(n_total_hours), 
            connection=engine
        )
        df_top_owned = pl.read_database(
            query=top_n_accum(
                n_total_owned,
                column=GamesData.game_code,
                col_label="game_code",
                accum_func=func.count,
                accum_col=GamesData.profile_code,
                accum_label="num_of_players",
            ),
            connection=engine,
        )
        popular_games = (
            set(df_top_owned["game_code"].to_list())
            .union(set(df_top_played["game_code"].to_list()))
        )
        self.game_codes = list(set(self.history_dict.keys()).union(popular_games))
    
    def get_percentiles(self, percentiles = [0.2, 0.4, 0.6, 0.8]): 
        self.percentiles = percentiles
        self.df_hour_percentiles = pl.read_database(
            query = hour_percentiles_for_games(self.game_codes, q_list=self.percentiles),
            connection=engine
            )
        
    def get_players_histories(self, **kwargs):
        if not hasattr(self.user, 'df_user_ids_close_to_user'):
            self.user.find_close_users(**kwargs)
        profile_codes = self.user.df_user_ids_close_to_user.top_k(
            k=self.players_sample_size, by='distr_dist')['profile_code'].to_list()
        profile_codes = [i for i in profile_codes if i != self.user.user_id]

        self.players_history = pl.read_database(
            query=players_history_in_games(
                game_codes=self.game_codes,
                profile_codes=profile_codes,
            ),
            connection=engine,
        )
        self.df_ranks = map_history_to_ranks_from_percentiles(
            self.players_history, self.df_hour_percentiles)
    
    def generate_ranks(self,**kwargs):
        self.join_more_games()
        self.get_percentiles()
        self.get_players_histories(**kwargs)

    # M_incoplete preparation:
    ## 0. Get the full data about client for comparison 
    