from abc import ABC, abstractmethod
import polars as pl

from steam_players_orm.db_model import engine
from steam_players_orm.db_queries import players_history_in_games

class HistoryOfUserStrategy(ABC):
    @abstractmethod
    def get(self, **kwargs):
        pass

class HistoryOfUserInGames(HistoryOfUserStrategy):
    def get(self, game_codes:list, profile_codes:list):
        df_client_record = pl.read_database(
            query=players_history_in_games(game_codes, profile_codes),
            connection=engine.connect()
            )
        return df_client_record