from games_prediction.strategy_factory import StrategyFactory
factory = StrategyFactory()

options = {
    'game_selection_strategy' : factory.create_game_selection('popular-games', n_total_hours=50, n_total_owned=50),
    'game_naming_strategy' : factory.create_naming('names-from-db'),
    'traning_users_selection_strategy' : factory.create_traning_users('close-in-l1', num_of_results=100),
    'traning_users_data_strategy' : factory.create_traning_data('quantile-ranking', percentiles=[0.2, 0.4, 0.6, 0.8]),
    'joining_strategy' : factory.create_join('last-column'),
    'matrix_filling_strategy' : factory.create_filling('CSMC', col_fraction=0.2),
    'normalisation_strategy' : factory.create_normalisation('linear-rank', max_rank_value=4),
    'testing_strategy' : factory.create_test('statistical-tests'),
    'visualization_strategy' : factory.create_visualization('vertical-bar-plot')
}