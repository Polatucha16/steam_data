"""
Assume:
we have a sql table with header and example row:
| profile_code (int) | hours_on_record (int) | game_code (int) |
| 455798 | 47 | 66984 |
and we do the following steps :
1) pick a profile_code <- lest say it is fixed for now 
2) select 10 most popular games for this user and join this list of games with some popular games overall (for example most owned and most played)
3) find the users that are the closest in distribution of games to the user in the chosen 10 games that we picked up in the point 2).
4) for our user we put None in places apart from 10 games we selected at 2) 
5) make a history matrix out of users from point 3) joined with the user from point 4)
6) use low rank matrix filling algoritm to fill None places of matrix at 5)
7) compare the true history to results from matrix filling for the user we piked up at very beginning.

Propose the best design patterns in python for a flexible experiments in the points from 2) to 7). 
For example to change a way of picking more games in the point 2) from most popular to random, or replace matrix filling algoritm to other one.
ANSWER:
"""

from abc import ABC, abstractmethod

class GameSelectionStrategy(ABC):
    @abstractmethod
    def select_games(self, user_id, num_games):
        pass

class MostPopularGameStrategy(GameSelectionStrategy):
    def select_games(self, user_id, num_games):
        # Implement logic to select the most popular games
        pass

class RandomGameStrategy(GameSelectionStrategy):
    def select_games(self, user_id, num_games):
        # Implement logic to select random games
        pass

class MatrixFillingStrategy(ABC):
    @abstractmethod
    def fill_matrix(self, matrix):
        pass

class LowRankMatrixFillingStrategy(MatrixFillingStrategy):
    def fill_matrix(self, matrix):
        # Implement low-rank matrix filling algorithm
        pass

class AnotherMatrixFillingStrategy(MatrixFillingStrategy):
    def fill_matrix(self, matrix):
        # Implement another matrix filling algorithm
        pass

#### THEN 

class StrategyFactory:
    def create_game_selection_strategy(self, strategy_type):
        if strategy_type == 'most_popular':
            return MostPopularGameStrategy()
        elif strategy_type == 'random':
            return RandomGameStrategy()
        # Add more strategies as needed

    def create_matrix_filling_strategy(self, strategy_type):
        if strategy_type == 'low_rank':
            return LowRankMatrixFillingStrategy()
        elif strategy_type == 'another':
            return AnotherMatrixFillingStrategy()
        # Add more strategies as needed

## THEN

class ExperimentFacade:
    def __init__(self, game_selection_strategy, matrix_filling_strategy):
        self.game_selection_strategy = game_selection_strategy
        self.matrix_filling_strategy = matrix_filling_strategy

    def run_experiment(self, user_id, num_games):
        # Step 1: Pick a profile_code
        profile_code = user_id

        # Step 2: Select games
        selected_games = self.game_selection_strategy.select_games(user_id, num_games)

        # Step 3: Find closest users
        closest_users = find_closest_users(profile_code, selected_games)

        # Step 4: Create user history matrix
        user_history_matrix = create_user_history_matrix(profile_code, selected_games, closest_users)

        # Step 5: Create history matrix
        history_matrix = create_history_matrix(user_history_matrix)

        # Step 6: Fill matrix
        filled_matrix = self.matrix_filling_strategy.fill_matrix(history_matrix)

        # Step 7: Compare true history with filled matrix
        compare_results(user_history_matrix, filled_matrix)

## AND FINALLY

factory = StrategyFactory()
game_selection_strategy = factory.create_game_selection_strategy('most_popular')
matrix_filling_strategy = factory.create_matrix_filling_strategy('low_rank')

experiment_facade = ExperimentFacade(game_selection_strategy, matrix_filling_strategy)
experiment_facade.run_experiment(user_id, num_games)