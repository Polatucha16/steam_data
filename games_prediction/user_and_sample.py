from abc import ABC, abstractmethod
import polars as pl

from steam_players_orm.db_model import engine
from steam_players_orm.db_queries import player_history
from steam_players_orm.polars_funcs import df_to_id_val_dict

class UserAndSampleStrategy(ABC):
    @abstractmethod
    def pick(self, **kwargs):
        pass

class KMostPopularGamesOfUser(UserAndSampleStrategy):
    def __init__(self, user_id=2584514, top=10):
        self.user_id = user_id
        self.top = top
    def pick(self):
        self.df_client_record = pl.read_database(
            query=player_history(self.user_id, self.top),
            connection=engine.connect()
            )

        self.history_dict = df_to_id_val_dict(self.df_client_record, self.top)
        return self.user_id, self.history_dict