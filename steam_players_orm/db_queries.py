import re
import sqlalchemy as sa
from sqlalchemy import func, select, distinct, join, cast, case, Float
from sqlalchemy.sql.functions import _FunctionGenerator
from steam_players_orm.db_model import GamesData, GameCodes, NonzeroPlayers


def top_n_accum(
    n: int,
    column=GamesData.game_code,
    col_label="game_code",
    accum_func:_FunctionGenerator=func.sum,
    accum_col=GamesData.hours_on_record,
    accum_label="hours_on_record"
):
    query = (
        select(column.label(col_label), accum_func(accum_col).label(accum_label))
        .group_by(column)
        .order_by(sa.desc(accum_label))
        .limit(n)
    )
    return query


# def distribution_distance_query(input_dict:dict):
#     """ For input_dict = {gc_0 : hor_0, gc_1 : hor_1, ...} that represent gameplay history name it : "history_of_X"
#         funtion returns a SELECT query that return a table
#         with columns :
#             profile_code,
#             diff
#         ordered by diff, such that next to the profile_code there is accumulated difference between
#             games of profile_code
#             and
#             history_of_X
#         in games from history_of_X.
#     PREVIEW:
#         from sqlalchemy import select
#         from sqlalchemy.dialects import mssql

#         game_code_and_hours = {8025: 400, 708: 2, 57411: 3}
#         print(distribution_distance_query(game_code_and_hours)
#             .compile(
#                 dialect=mssql.dialect(),
#                 compile_kwargs={"literal_binds": True}
#                 )
#         )
#     """

#     def diff_col_sum(game_code_and_hours:dict):
#         """Generates: SUM (CASE + CASE + CASE + ...)
#         that aggregate distances only to game_codes from the game_code_and_hours.keys() """
#         list_of_stmt = [
#             f"ABS(CASE WHEN game_code = {game_code} THEN hours_on_record - {hours} ELSE {hours} END)"
#             for game_code, hours in game_code_and_hours.items()
#         ]
#         return "SUM (" + " + ".join(list_of_stmt) + ")"

#     stmt = (
#         select(
#         GamesData.profile_code,
#         sa.literal_column(text=diff_col_sum(input_dict)).label("diff")
#         )
#         .where(GamesData.game_code.in_([*input_dict.keys()]))
#         .group_by(GamesData.profile_code)
#         .order_by(sa.column("diff"))
#     )
#     return stmt


def distribution_distance_query(input_dictionary: dict, input_total=None, num_of_results=None):
    """ 
    Statistical distance (L1) between 
        distribution of hours_on_record among game_code in input_dictionary
    and 
        corresponding distributions for profile_codes in GamesData

    (***)
    can be changed to stat. dist. between 
        input_dictionary {cat:val, ...}
    and 
        users represented by ids in table
    
    ids, cat, val
     ⋮     ⋮    ⋮
    """

    input_total = sum(input_dictionary.values()) if input_total is None else input_total

    case_list = [
        (
            GamesData.game_code == game_code,
            func.abs(
                (
                    cast(GamesData.hours_on_record, Float)
                    / cast(NonzeroPlayers.total_hours, Float)
                )
                - (cast(hours, Float) / cast(input_total, Float))
            ),
        )
        for game_code, hours in input_dictionary.items()
    ]

    sum_column = func.sum(
        case(
            *case_list,
            else_=func.abs(
                cast(GamesData.hours_on_record, Float)
                / cast(NonzeroPlayers.total_hours, Float)
            ),
        )
    )

    query = (
        select(
            GamesData.profile_code,
            sum_column.label("distr_dist"),
        )
        .select_from(
            GamesData.__table__.join(
                NonzeroPlayers.__table__,
                GamesData.profile_code == NonzeroPlayers.profile_code,
            )
        )
        .group_by(GamesData.profile_code)
        .order_by("distr_dist")
        .limit(num_of_results)
    )

    return query


def hour_percentiles_for_games(
    game_codes_list: list, q_list: list = [0.2, 0.4, 0.6, 0.8]
):
    """This query will calculate the percentiles specified in q_list
    of the hours_on_record column for each game_code specified in the game_codes_list
    output tabe will have:
    number of rows =    |game_codes_list|,
    number of columns = |q_list|
    """
    quantile_columns = [
        func.percentile_cont(q)
        .within_group(GamesData.hours_on_record)
        .over(partition_by=GamesData.game_code)
        .label(f"q_{int(100*q)}")
        for q in q_list
    ]
    query = (
        select(
            distinct(GameCodes.game_code),
            *quantile_columns,
        )
        .select_from(
            join(
                GameCodes,
                GamesData,
                GameCodes.game_code == GamesData.game_code,
                isouter=True,
            )
        )
        .where(GameCodes.game_code.in_(game_codes_list))
    )

    return query

def player_history(profile_code, top=None):
    query = (
        select(GamesData.profile_code, GamesData.game_code, GamesData.hours_on_record)
        .filter(GamesData.profile_code == profile_code)
        .order_by(GamesData.hours_on_record.desc())
    )
    if top is not None:
        query = query.limit(top)
    return query

def players_history_in_games(game_codes: list, profile_codes:list=None):
    """ Returns a query that selects profile_code and hours_on_record for each game_code in game_codes.
    Args:
        game_codes (list): A list of game codes.
    Returns:
        A SQLAlchemy query object.
    """

    # a column for each game_code in game_codes
    case_statements = [
        func.sum(
            sa.case((GamesData.game_code == game_code, GamesData.hours_on_record), else_=0)
        ).label(f"{game_code}")
        for game_code in game_codes
    ]

    query = (
            select(GamesData.profile_code, *case_statements)
                .group_by(GamesData.profile_code)
    )
    
    # Filter by profile codes if provided
    if profile_codes:
        query = query.filter(GamesData.profile_code.in_(profile_codes))

    return query

# region NOOB code below
# def query_for_gamecodes(code_list):
#     """function that for example list [8025, 8449, 18723] generates a query :
#     WITH
#         g0 AS (
#         SELECT TOP 10000 profile_code, hours_on_record
#         FROM steam_players.dbo.games_data
#         WHERE game_code = 18723
#         ),
#         g1 AS (
#         SELECT TOP 10000 profile_code, hours_on_record
#         FROM steam_players.dbo.games_data
#         WHERE game_code = 8449
#         ),
#         g2 AS (
#         SELECT TOP 10000 profile_code, hours_on_record
#         FROM steam_players.dbo.games_data
#         WHERE game_code = 8025
#         ),
#         union_of_profile_codes AS (
#         SELECT profile_code FROM g0
#         UNION
#         SELECT profile_code FROM g1
#         UNION
#         SELECT profile_code FROM g2
#         )
#     SELECT
#         union_of_profile_codes.profile_code AS profile_code,
#         COALESCE(g0.hours_on_record, 0) AS game_0,
#         COALESCE(g1.hours_on_record, 0) AS game_1,
#         COALESCE(g2.hours_on_record, 0) AS game_2
#     FROM
#         union_of_profile_codes
#     LEFT JOIN g0
#     ON union_of_profile_codes.profile_code = g0.profile_code
#     LEFT JOIN g1
#     ON union_of_profile_codes.profile_code = g1.profile_code
#     LEFT JOIN g2
#     ON union_of_profile_codes.profile_code = g2.profile_code;
#     """

#     def gi_subtable(gamecode:int):
#         sub_table = re.sub('[ \t]+\t', '\t', f"""AS (
#             \tSELECT TOP 10000 profile_code, hours_on_record
#             \tFROM steam_players.dbo.games_data
#             \tWHERE game_code = {gamecode}
#             \t)"""
#        )
#         return sub_table

#     def with_query_list_part(code_list):
#         return ",\n\t".join([f"g{i} " + gi_subtable(game_code) for i, game_code in enumerate(code_list)])

#     def with_query_union_part(code_list):
#         union_name = ",\n\tunion_of_profile_codes AS (\n"
#         union_list = "\n\tUNION\n".join(
#             [f"\tSELECT profile_code FROM g{i}" for i,game_code in enumerate(code_list)]
#         )
#         return union_name + union_list + "\n\t)"

#     def query_with_part(code_list):
#         return "WITH \n\t" + with_query_list_part(code_list) + with_query_union_part(code_list)

#     def query_select_columns(code_list):
#         union_codes_column = "\n\tunion_of_profile_codes.profile_code AS profile_code,\n\t"
#         columns_list = [f"COALESCE(g{i}.hours_on_record, 0) AS game_{i}" for i,game_code in enumerate(code_list)]
#         from_part = "\nFROM\n\tunion_of_profile_codes"
#         return "SELECT" + union_codes_column + ",\n\t".join(columns_list) + from_part

#     def left_union_part(code_list):
#         union_list = [f"LEFT JOIN g{i}\nON union_of_profile_codes.profile_code = g{i}.profile_code" for i, game_code in enumerate(code_list) ]
#         return "\n".join(union_list)+";"

#     return query_with_part(code_list)+"\n"+query_select_columns(code_list)+"\n"+left_union_part(code_list)

# def gen_close_total_query(num_of_players:int, client_total_hours:int):
#     query = (
#         f"SELECT TOP {num_of_players}"
#             f"\n\tprofile_code,"
#             f"\n\tSUM(hours_on_record)-{client_total_hours} AS hours_diff"
#         f"\nFROM steam_players.dbo.games_data"
#         f"\nGROUP BY profile_code"
#         f"\nORDER BY ABS(SUM(hours_on_record)-{client_total_hours}) ASC;"
#     )
#     return query
#
#### MORE NOOB 1
# def gen_close_total_query(num_of_players:int, client_total_hours:int):
#     query = (
#         f"SELECT TOP {num_of_players}"
#             f"\n\tprofile_code," 
#             f"\n\tSUM(hours_on_record)-{client_total_hours} AS hours_diff"
#         f"\nFROM steam_players.dbo.games_data"
#         f"\nGROUP BY profile_code"
#         f"\nORDER BY ABS(SUM(hours_on_record)-{client_total_hours}) ASC;"
#     )
#     return query
# # print(gen_close_total_query(50, 5009))

# def close_in_total():
    
#     return 0

# num_of_players = 50

# df_close_total = pl.read_database(
#     query=gen_close_total_query(num_of_players, client_total), 
#     connection=engine.connect())

# print(df_close_total.select(['profile_code']).to_series().to_list())

# #### MORE NOOB 2
# def profiles_vs_games_query(profile_codes_list, game_codes_list):
#     source_name = "top_source"
#     def top_source(profile_codes_list, game_codes_list, source_name=source_name):
#         query = (
#             f"{source_name} AS ("
#             f"\n\tSELECT"
#             f"\n\t\t[profile_code],"
#             f"\n\t\t[game_code],"
#             f"\n\t\t[hours_on_record]"
#             f"\n\tFROM [steam_players].[dbo].[games_data]"
#             f"\n\tWHERE	"
#             f"\n\t\t[game_code] IN {*game_codes_list,}"
#             f"\n\tAND"
#             f"\n\t\t[profile_code] IN {*profile_codes_list,}"
#             "\n\t)"
#         )
#         return query
    
#     def union_of_profile_codes(source_name=source_name):
#         query = (
#             f"union_of_profile_codes AS ("
# 	        f"\n\tSELECT DISTINCT [profile_code]"
# 	        f"\n\tFROM {source_name}"
#             f")"
#         )
#         return query
    
#     def gi_game(game_code:int, source_name=source_name):
#         query = ( 
#             f" AS ("
#             f"\n\tSELECT [profile_code], [hours_on_record]"
#             f"\n\tFROM {source_name}"
#             f"\n\tWHERE game_code IN ({game_code})"
#             f"\n\t)"""
#         )
#         return query
    
#     def with_query_list_part(profile_codes_list,game_codes_list):
#         query = (
#             "WITH" + "\n" +
#             top_source(profile_codes_list, game_codes_list) + ",\n" +
#             union_of_profile_codes() + ",\n" +
#             ",\n".join([f"g_{game_code} " + gi_game(game_code) for i, game_code in enumerate(game_codes_list)])
#         )
#         return query
    
#     def select_columns(game_codes_list):
#         column_list = [
#                 f"COALESCE(g_{game_code}.hours_on_record, 0) AS game_{game_code}" 
#                 for i, game_code in enumerate(game_codes_list)
#                 ]
#         query = (
#             "\nSELECT"
#             f"\n\tunion_of_profile_codes.profile_code AS profile_code,\n\t" +
#             ",\n\t".join(column_list)
#         )
#         return query
#     def from_query(game_codes_list):
#         query = (
#             "\nFROM"
#             f"\n\tunion_of_profile_codes"
#         )
#         left_union_list = [
#             f"LEFT JOIN g_{game_code}\nON union_of_profile_codes.profile_code = g_{game_code}.profile_code"
#             for game_code in game_codes_list
#         ]
#         return query + "\n" + "\n".join(left_union_list)
#     return (
#         with_query_list_part(profile_codes_list, game_codes_list) + 
#         select_columns(game_codes_list) + 
#         from_query(game_codes_list) + ";"
#         )

# endregion
