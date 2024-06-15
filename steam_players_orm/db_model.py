import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.dialects.mssql import NVARCHAR

server = 'localhost'
database = 'steam_players'
driver = 'ODBC Driver 17 for SQL Server'

engine = create_engine(f'mssql://{server}:1433/{database}?trusted_connection=yes&driver={driver}')

Base = declarative_base()

class GamesData(Base):
    __tablename__ = 'games_data'
    __table_args__ = {'schema': 'dbo'}

    hours_last_2_weeks = sa.Column("hours_last_2_weeks", sa.BigInteger)
    hours_on_record = sa.Column("hours_on_record", sa.BigInteger)
    profile_code = sa.Column("profile_code", sa.Integer, primary_key=True)
    game_code = sa.Column("game_code", sa.Integer, primary_key=True)

class GameCodes(Base):
    __tablename__ = 'game_codes'
    __table_args__ = {'schema': 'dbo'}

    game_code = sa.Column("game_code", sa.Integer, primary_key=True)
    game_name = sa.Column("game_name", NVARCHAR(None))

class NonzeroPlayers(Base):
    __tablename__ = 'nonzero_players'
    __table_args__ = {'schema': 'dbo'}

    profile_code = sa.Column("profile_code", sa.Integer, primary_key=True)
    total_hours = sa.Column("total_hours", sa.BigInteger)