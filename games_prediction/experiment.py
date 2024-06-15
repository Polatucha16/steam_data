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
from games_prediction.performance_test import TestStrategy
from games_prediction.game_names import NamingStrategy
from games_prediction.visualization import VisualizationStrategy

class Experiment:
    def __init__(
        self,
        pick_user_and_sample_strategy : UserAndSampleStrategy,
        game_selection_strategy : GameSelectionStrategy,
        game_naming_strategy : NamingStrategy,
        traning_users_selection_strategy : TraningUsersStrategy,
        traning_users_data_strategy : TraningDataStrategy,
        joining_strategy : JoiningStrategy,
        matrix_filling_strategy : FillingStrategy,
        normalisation_strategy : NormalisationStrategy,
        testing_strategy : TestStrategy,
        visualization_strategy : VisualizationStrategy
    ):
        # Strategies
        self.pick_user_and_sample_strategy = pick_user_and_sample_strategy
        self.game_selection_strategy = game_selection_strategy
        self.game_naming_strategy = game_naming_strategy
        self.traning_users_selection_strategy = traning_users_selection_strategy
        self.traning_users_data_strategy = traning_users_data_strategy
        self.joining_strategy = joining_strategy
        self.matrix_filling_strategy = matrix_filling_strategy
        self.normalisation_strategy = normalisation_strategy
        self.testing_strategy = testing_strategy
        self.visualization_strategy = visualization_strategy
        
        # DB Connection
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

        # Step 2.75: get the names of selected games
        self.games_names : list
        self.games_names = self.game_naming_strategy.get(self.selected_games)

        # Step 3: Find users that we are going to learn from.
        self.traning_users : pl.DataFrame # with 'profile_code' column
        self.traning_users = self.traning_users_selection_strategy.select_users(
            self.games_sample)

        # Step 4: Create traning data (ranked_history of tranming users) & data for sample

        self.traning_users_list = self.traning_users["profile_code"].to_list()
        if self.user_code in self.traning_users_list:
            self.traning_users_list.remove(self.user_code)
        self.traning_users_data: pl.DataFrame
        self.traning_users_data = self.traning_users_data_strategy.data_for_users(
            self.traning_users_list, self.selected_games
        )

        self.user_ranks : pl.DataFrame
        self.user_ranks = self.traning_users_data_strategy.data_for_sample(self.user_code, self.games_sample)

        # Step 4.5 check the order of games in dataframe
        assert (
            list(map(int, self.traning_users_data.columns[1:])) == self.selected_games
        ), """Games from TraningDataStrategy.prepare 
        do not match selected_games in number or in order"""

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
        self.predicted_ranks : np.ndarray
        self.predicted_ranks = self.normalisation_strategy.normalise(self.predictions_for_user, self.inds_to_predict)

        # & transform predicted_ranks to pl.DataFrame
        self.pred_user_ranks : pl.DataFrame
        self.pred_user_ranks = pl.from_dict({str(game_id) : [rank] for game_id, rank in zip(self.selected_games, self.predicted_ranks)})

        # Step 9: get the true ranks of a user for comparison
        self.true_user_ranks : pl.DataFrame
        self.true_user_ranks = (
            self.traning_users_data_strategy
                .data_for_users([self.user_code], self.selected_games)
                .select(pl.exclude("profile_code"))
                )
        # Step 9.5  true and predictions pl.Dataframes restricted to {selected_games}-{sample}
        self.predictions = self.pred_user_ranks.select(pl.exclude(list(map(str, self.games_sample.keys()))))
        self.true_history = self.true_user_ranks.select(pl.exclude(list(map(str, self.games_sample.keys()))))

        # Step 10: Compare true history with predictions
        self.test_results : dict
        self.test_results = self.testing_strategy.test(self.predictions , self.true_history)

        # Step 11: visualization

    def plot(self):
        self.visualization_strategy.plot(
            predicted=self.predictions.to_numpy().reshape(-1),
            true_values=self.true_history.to_numpy().reshape(-1),
            game_names=[self.games_names[idx] for idx in self.inds_to_predict],
        )

#region lists_comparison 
# from games_prediction.experiment_settings import options

# from games_prediction.experiment import Experiment
# from games_prediction.strategy_factory import StrategyFactory

# factory = StrategyFactory()

# experiment = Experiment(
#     pick_user_and_sample_strategy = factory.create_sample('k-top-from-user', user_id=427274, top=15),
#     game_selection_strategy = factory.create_game_selection('popular-games', n_total_hours=50, n_total_owned=50),
#     game_naming_strategy = factory.create_naming('names-from-db'),
#     traning_users_selection_strategy = factory.create_traning_users('close-in-l1', num_of_results=100),
#     traning_users_data_strategy = factory.create_traning_data('quantile-ranking', percentiles=[0.2, 0.4, 0.6, 0.8]),
#     joining_strategy = factory.create_join('last-column'),
#     matrix_filling_strategy = factory.create_filling('CSMC', col_fraction=0.2),
#     normalisation_strategy = factory.create_normalisation('linear-rank', max_rank_value=4),
#     testing_strategy = factory.create_test('statistical-tests'),
#     visualization_strategy = factory.create_visualization('vertical-bar-plot')
# )
# experiment.run()

# A=history_for_users i B=df_hour_percentiles mają #(A\B) > 0  i #(B\A) > 0
# proof:
# set(experiment.traning_users_data_strategy.df_hour_percentiles['game_code'].to_list()).difference(set(list(map(int,experiment.traning_users_data_strategy.history_for_users.columns[1:]))))
# set(list(map(int,experiment.traning_users_data_strategy.history_for_users.columns[1:]))).difference(set(experiment.traning_users_data_strategy.df_hour_percentiles['game_code'].to_list()))
#
# Jak mają się A i B do S=obj.selected_games : #(S\B)>0 oraz A = S
# proof:
# set(experiment.selected_games).difference(set(experiment.traning_users_data_strategy.df_hour_percentiles['game_code'].to_list()))
# set(list(map(int,experiment.traning_users_data_strategy.history_for_users.columns[1:]))).difference(set(experiment.selected_games))

# import polars as pl
# from steam_players_orm.db_model import engine
# from steam_players_orm.db_queries import hour_percentiles_for_games
# # hour_percentiles_for_games([1282, 23, 17])
# df_in_Q = pl.read_database(
#             query=hour_percentiles_for_games(experiment.selected_games),
#             connection=engine,
#         )

# set(experiment.selected_games).difference(set(df_in_Q['game_code'].to_list()))
#endregion