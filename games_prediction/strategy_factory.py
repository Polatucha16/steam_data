from games_prediction.user_and_sample import KMostPopularGamesOfUser
from games_prediction.game_selection import MostPopular
from games_prediction.traning_users import CloseInL1
from games_prediction.traning_data_strategies import QuantileRanking
from games_prediction.sample_data_join import LastColumn
from games_prediction.filling import FillWithCSMC
from games_prediction.normalisation import NormaliseRanks
from games_prediction.history_of_user import HistoryOfUserInGames

"""
pick_user_and_sample_strategy,
game_selection_strategy,
traning_users_selection_strategy,
traning_users_data_strategy,
joining_strategy,
matrix_filling_strategy,
performance_test_strategy
"""
class StrategyFactory:

    def create_sample(self, strategy_name, **kwargs):
        self.sample_strategies = ["k-top-from-user"]
        if strategy_name == "k-top-from-user":
            return KMostPopularGamesOfUser(**kwargs)
        else:
            raise Exception(
                f"No strategy named {strategy_name}.\nAvailable strategies are: {', '.join(self.sample_strategies)}"
            )

    def create_game_selection(self, strategy_name, **kwargs):
        self.game_selection_strategies = ['popular-games']
        if strategy_name == 'popular-games':
            return MostPopular(**kwargs)
        else:
            raise Exception(
                f"No strategy named {strategy_name}.\nAvailable strategies are: {', '.join(self.game_selection_strategies)}"
            )

    def create_traning_users(self, strategy_name, **kwargs):
        self.traning_users_strategies = ['close-in-l1']
        if strategy_name == 'close-in-l1':
            return CloseInL1(**kwargs)
        else:
            raise Exception(
                f"No strategy named {strategy_name}.\nAvailable strategies are: {', '.join(self.traning_users_strategies)}"
            )

    def create_traning_data(self, strategy_name, **kwargs):
        self.traning_data_strategies = ['quantile-ranking']
        if strategy_name == 'quantile-ranking':
            return QuantileRanking(**kwargs)
        else:
            raise Exception(
                f"No strategy named {strategy_name}.\nAvailable strategies are: {', '.join(self.traning_data_strategies)}"
            )

    def create_join(self, strategy_name, **kwargs):
        self.join_strategies = ['last-column']
        if strategy_name == 'last-column':
            return LastColumn(**kwargs)
        else:
            raise Exception(
                f"No strategy named {strategy_name}.\nAvailable strategies are: {', '.join(self.join_strategies)}"
            )

    def create_filling(self, strategy_name, **kwargs):
        self.filling_strategies = ['CSMC']
        if strategy_name == 'CSMC':
            return FillWithCSMC(**kwargs)
        else:
            raise Exception(
                f"No strategy named {strategy_name}.\nAvailable strategies are: {', '.join(self.filling_strategies)}"
            )

    def create_normalisation(self, strategy_name, **kwargs):
        self.normalisation_strategies = ['linear-rank']
        if strategy_name == 'linear-rank':
            return NormaliseRanks(**kwargs)
        else:
            raise Exception(
                f"No strategy named {strategy_name}.\nAvailable strategies are: {', '.join(self.normalisation_strategies)}"
            )

    # def create_history_search(self, strategy_name, **kwargs):
    #     self.history_search_strategies = ['user-in-games']
    #     if strategy_name == 'user-in-games':
    #         return HistoryOfUserInGames(**kwargs)
    #     else:
    #         raise Exception(
    #             f"No strategy named {strategy_name}.\nAvailable strategies are: {', '.join(self.history_search_strategies)}"
    #         )

    # def create_matrix_filling_strategy(self, strategy_type):
    #     if strategy_type == 'low_rank':
    #         return LowRankMatrixFillingStrategy()
    #     elif strategy_type == 'another':
    #         return AnotherMatrixFillingStrategy()
    #     # Add more strategies as needed
