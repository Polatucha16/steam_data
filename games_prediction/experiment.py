# from __future__ import known_unknown_locator
import polars as pl
from steam_players_orm.db_model import engine

from games_prediction.user_and_sample import UserAndSampleStrategy
from games_prediction.game_selection import GameSelectionStrategy
from games_prediction.traning_users import TraningUsersStrategy
from games_prediction.traning_data_strategies import TraningDataStrategy
from games_prediction.sample_data_join import JoiningStrategy
from games_prediction.filling import FillingStrategy

class Experiment:
    def __init__(
        self,
        pick_user_and_sample_strategy : UserAndSampleStrategy,
        game_selection_strategy : GameSelectionStrategy,
        traning_users_selection_strategy : TraningUsersStrategy,
        traning_users_data_strategy : TraningDataStrategy,
        joining_strategy : JoiningStrategy,
        matrix_filling_strategy : FillingStrategy,
        # normalisation_strategy
        # performance_test_strategy
    ):
        self.pick_user_and_sample_strategy = pick_user_and_sample_strategy
        self.game_selection_strategy = game_selection_strategy
        self.traning_users_selection_strategy = traning_users_selection_strategy
        self.traning_users_data_strategy = traning_users_data_strategy
        self.joining_strategy = joining_strategy
        self.matrix_filling_strategy = matrix_filling_strategy
        # self.performance_test_strategy = performance_test_strategy

        self.engine=engine

    def run(self):
        # Step 1: Pick a user and what we know about the user (int, dict {key=game_code : val=hours_on_record,...})
        self.user_code : int
        self.games_sample : list
        self.user_code, self.games_sample = self.pick_user_and_sample_strategy.pick()

        # # Step 2: Select what games are we going to predict
        self.selected_games : list
        self.selected_games = self.game_selection_strategy.select(self.games_sample)

        # # Step 3: Find users that we are going to learn from.
        self.traning_users : pl.DataFrame
        self.traning_users = self.traning_users_selection_strategy.select_users(
            self.games_sample)

        # # Step 4: Create user history matrix (return ranked_history of users)
        self.traning_users_data : pl.DataFrame
        self.user_ranks : pl.DataFrame
        self.traning_users_data, self.user_ranks = self.traning_users_data_strategy.prepare(
            self.user_code, 
            self.games_sample,
            self.traning_users, 
            self.selected_games
        )
        
        # (self.user_games_indices, self.to_predict_indices) = known_unknown_locator(
        #     self.games_sample, self.selected_games
        # )

        # Step 5: Create traning_matrix (numpy column users with Nones)
        self.M_incomplete = self.joining_strategy.join(
            self.user_ranks, 
            self.traning_users_data
        )

        # Step 6: Use predition algorithm
        self.M_filled = self.matrix_filling_strategy.fill(self.M_incomplete)

        # Step 7: normalise the result
        self.predicted_ranks : dict
        self.predicted_ranks = self.normalisation_strategy.normalise(self.M_filled)


        # Step 8: Compare true history with filled matrix
        # self.performance_result = self.performance_test_strategy.compare(self.M_filled,)
