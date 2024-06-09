from abc import ABC, abstractmethod
import numpy as np
import polars as pl
from sqlalchemy import select, case

from steam_players_orm.db_model import engine, GameCodes

class NamingStrategy(ABC):
    @abstractmethod
    def get(self, game_ids:list, **kwargs):
        pass

class GameNamesFromDB(NamingStrategy):

    def get(self, game_codes: list):
        id_ordering = case(
        {_id: index for index, _id in enumerate(game_codes)},
        value=GameCodes.game_code
        )

        query_name_with_code = (
        select(GameCodes.game_code, GameCodes.game_name)
        .filter(GameCodes.game_code.in_(game_codes))
        .order_by(id_ordering)
        )
        df_name_with_code = pl.read_database(
            query=query_name_with_code, connection=engine)
        game_names = df_name_with_code['game_name'].to_list()
        return game_names