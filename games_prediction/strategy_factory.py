from games_prediction.user_and_sample import KMostPopularGamesOfUser
from games_prediction.game_selection import MostPopular
from games_prediction.traning_users import CloseInL1
from games_prediction.traning_data_strategies import QuantileRanking
from games_prediction.sample_data_join import LastColumn
from games_prediction.filling import FillWithCSMC
from games_prediction.normalisation import NormaliseRanks
from games_prediction.performance_test import StatisticalTests
from games_prediction.game_names import GameNamesFromDB
from games_prediction.visualization import VerticalBarPlot

"""
pick_user_and_sample_strategy,
game_selection_strategy,
traning_users_selection_strategy,
traning_users_data_strategy,
joining_strategy,
matrix_filling_strategy,
performance_test_strategy
"""
def strategy_name_not_found(name_given, name_list):
    raise Exception(
            f"No strategy named {name_given}.\nAvailable strategies are: {', '.join(name_list)}"
        )

class StrategyFactory:

    def create_sample(self, strategy_name, **kwargs):
        self.sample_strategies = ["k-top-from-user"]
        if strategy_name == "k-top-from-user":
            return KMostPopularGamesOfUser(**kwargs)
        # elif strategy_name == 'another-strategy':
        #     return AnotherStrategy()
        # Add more strategies as needed
        else:
            strategy_name_not_found(strategy_name, self.sample_strategies)

    def create_game_selection(self, strategy_name, **kwargs):
        self.game_selection_strategies = ['popular-games']
        if strategy_name == 'popular-games':
            return MostPopular(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.game_selection_strategies)

    def create_traning_users(self, strategy_name, **kwargs):
        self.traning_users_strategies = ['close-in-l1']
        if strategy_name == 'close-in-l1':
            return CloseInL1(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.traning_users_strategies)

    def create_traning_data(self, strategy_name, **kwargs):
        self.traning_data_strategies = ['quantile-ranking']
        if strategy_name == 'quantile-ranking':
            return QuantileRanking(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.traning_data_strategies)

    def create_join(self, strategy_name, **kwargs):
        self.join_strategies = ['last-column']
        if strategy_name == 'last-column':
            return LastColumn(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.join_strategies)

    def create_filling(self, strategy_name, **kwargs):
        self.filling_strategies = ['CSMC']
        if strategy_name == 'CSMC':
            return FillWithCSMC(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.filling_strategies)

    def create_normalisation(self, strategy_name, **kwargs):
        self.normalisation_strategies = ['linear-rank']
        if strategy_name == 'linear-rank':
            return NormaliseRanks(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.normalisation_strategies)

    def create_test(self, strategy_name, **kwargs):
        self.test_strategies = ['statistical-tests']
        if strategy_name == 'statistical-tests':
            return StatisticalTests(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.test_strategies)
    
    def create_naming(self, strategy_name, **kwargs):
        self.naming_strategies = ['names-from-db']
        if strategy_name == 'names-from-db':
            return GameNamesFromDB(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.naming_strategies)

    def create_visualization(self, strategy_name, **kwargs):
        self.visualization_strategies = ['vertical-bar-plot']
        if strategy_name == 'vertical-bar-plot':
            return VerticalBarPlot(**kwargs)
        else:
            strategy_name_not_found(strategy_name, self.visualization_strategies)