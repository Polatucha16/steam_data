import polars as pl
from polars.dataframe.frame import DataFrame


def df_to_id_val_dict(
    df: DataFrame, top=10, col_with_keys="game_code", col_with_values="hours_on_record"
):
    """Return dictionary where a row is mapped to key:value pair.
    Takes two columns with names:
        col_with_keys and col_with_values,
        (make shure values in col_with_keys are unique)"""
    return {
        key: value
        for key, value in df.top_k(top, by=col_with_values)[
            [col_with_keys, col_with_values]
        ].iter_rows()
    }

def map_history_to_ranks_from_percentiles(history:DataFrame, percentiles:DataFrame, zero_q=0):
    """
    Map values in DF history to categories based on percentiles in DF percentiles.

    Args:
        percentiles (pl.DataFrame): Table with example columns: 'game_code', 'q_20', 'q_40', 'q_60', 'q_80'.
        table2 (pl.DataFrame): Table with columns: 'profile_code', and game_codes.

    Returns:
        pl.DataFrame: Table with history values mapped to ranks.
    """

    def row_of(df:DataFrame, game_code:int):
        return df.with_row_index().filter(pl.col('game_code') == game_code)['index'].item()
    
    def quant_of_game(q, game_code, df:DataFrame):
        return df.item(
            row=row_of(df, game_code), 
            column=f'q_{q}'
            )
    
    game_codes = history.select(pl.exclude("profile_code")).columns
    q_col_val_map = lambda x: int(x[2:]) # changes string 'q_X' to integer X
    quantile_values = list(map(q_col_val_map, percentiles.select(pl.exclude("game_code")).columns))
    expressions = []

    for game_code in game_codes:
        expr = None
        for i, quantile in enumerate(quantile_values + [quantile_values[-1]]):
            if i == 0:
                expr = pl.when(pl.col(game_code) < quant_of_game(quantile, int(game_code), percentiles)).then(i+zero_q)
            elif i == len(quantile_values):
                expr = expr.otherwise(i+zero_q)
            else :
                expr = expr.when(pl.col(game_code) < quant_of_game(quantile, int(game_code), percentiles)).then(i+zero_q)
        expressions.append(expr.alias(game_code))

    return history.with_columns(expressions)