# from __future__ import known_unknown_locator
import polars as pl
import numpy as np

from steam_players_orm.db_model import engine

from games_prediction.user_and_sample import UserAndSampleStrategy
from games_prediction.game_selection import GameSelectionStrategy
from games_prediction.traning_users import TraningUsersStrategy
from games_prediction.traning_data_strategies import TraningDataStrategy
from games_prediction.sample_data_join import JoiningStrategy
from games_prediction.filling import FillingStrategy
from games_prediction.normalisation import NormalisationStrategy

class Experiment:
    def __init__(
        self,
        pick_user_and_sample_strategy : UserAndSampleStrategy,
        game_selection_strategy : GameSelectionStrategy,
        traning_users_selection_strategy : TraningUsersStrategy,
        traning_users_data_strategy : TraningDataStrategy,
        joining_strategy : JoiningStrategy,
        matrix_filling_strategy : FillingStrategy,
        normalisation_strategy : NormalisationStrategy,
        # performance_test_strategy
    ):
        self.pick_user_and_sample_strategy = pick_user_and_sample_strategy
        self.game_selection_strategy = game_selection_strategy
        self.traning_users_selection_strategy = traning_users_selection_strategy
        self.traning_users_data_strategy = traning_users_data_strategy
        self.joining_strategy = joining_strategy
        self.matrix_filling_strategy = matrix_filling_strategy
        self.normalisation_strategy = normalisation_strategy
        # self.history_of_user_strategy = history_of_user_strategy
        # self.performance_test_strategy = performance_test_strategy

        self.engine=engine

    def run(self):
        # Step 1: Pick a user and what we know about the user
        self.user_code : int        # as is in db
        self.games_sample : dict    # {key=game_code : val=hours_on_record, ...}
        self.user_code, self.games_sample = self.pick_user_and_sample_strategy.pick()

        # Step 2: Select what games are we going to predict
        self.selected_games : list
        self.selected_games = self.game_selection_strategy.select(self.games_sample)

        # Step 2.5: inds in selected_games where games from games_sample are located and inds of games to predict values for a user
        self.inds_of__sample : np.ndarray
        self.inds_to_predict : np.ndarray
        self.inds_of__sample = np.where(list(map(lambda x: x in set(self.games_sample.keys()), self.selected_games)))[0]
        self.inds_to_predict = np.where(list(map(lambda x: x not in set(self.games_sample.keys()), self.selected_games)))[0]

        # Step 3: Find users that we are going to learn from.
        self.traning_users : pl.DataFrame
        self.traning_users = self.traning_users_selection_strategy.select_users(
            self.games_sample)

        # Step 4: Create traning data (ranked_history of tranming users) & data for sample

        traning_users_list = self.traning_users["profile_code"].to_list()
        if self.user_code in traning_users_list:
            traning_users_list.remove(self.user_code)
        self.traning_users_data: pl.DataFrame
        self.traning_users_data = self.traning_users_data_strategy.data_for_users(
            traning_users_list, self.selected_games
        )

        self.user_ranks : pl.DataFrame
        self.user_ranks = self.traning_users_data_strategy.data_for_sample(self.user_code, self.games_sample)

        # Step 4.5 check the order of games
        assert list(map(int, self.traning_users_data.columns[1:])) == self.selected_games, """Games from TraningDataStrategy.prepare 
        do not match selected games in number or in order"""

        # Step 5: Create traning_matrix (numpy column users with Nones)
        self.M_incomplete : np.ndarray
        self.M_incomplete = self.joining_strategy.join(
            self.user_ranks, 
            self.traning_users_data
        )

        # Step 6: Use predition algorithm
        self.M_filled : np.ndarray
        self.M_filled = self.matrix_filling_strategy.fill(self.M_incomplete)

        # Step 7: Exctract the predictions
        self.predictions_for_user : np.ndarray
        self.predictions_for_user = self.joining_strategy.extract(self.M_filled)

        # Step 8: normalise the result
        self.predicted_ranks : dict
        self.predicted_ranks = self.normalisation_strategy.normalise(self.predictions_for_user, self.inds_to_predict)

        # Step 9: get the true ranks of a user for comparison
        self.true_user_ranks = self.traning_users_data_strategy.data_for_users([self.user_code], self.selected_games)


        # Step 8: Compare true history with filled matrix
        # self.performance_result = self.performance_test_strategy.compare(self.M_filled,)
