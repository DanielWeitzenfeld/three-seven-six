from __future__ import division
import datetime
from dateutil.relativedelta import relativedelta

from sqlalchemy import func, select, and_, or_, text, Text, Column, Integer, ForeignKey, String, Table, DateTime, Date, \
    Numeric, Boolean, cast, Float, ForeignKeyConstraint
from sqlalchemy import Column, Integer, ForeignKey, String, Table
from sqlalchemy.orm import relationship, aliased
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.sql.expression import case

import three_seven_six
from three_seven_six.dbs import mysql
from .mysql_base import MysqlBase

INTERRUPTED_SEASON_THRESHOLD = 40


class Player(MysqlBase):
    __tablename__ = "players"
    id = Column(Integer, primary_key=True, autoincrement=True)
    player = Column(Text)
    player_key = Column(String(20), index=True)
    from_year = Column(Integer)
    to_year = Column(Integer)
    pos = Column(Text)
    weight = Column(Integer)
    birth_date = Column(Date)
    college = Column(Text)
    height = Column(Integer)
    link = Column(Text)
    scraped = Column(Boolean)

    seasons = relationship("Season", cascade="all", backref='player')

    URL_ROOT = 'http://www.basketball-reference.com'

    @hybrid_property
    def url(self):
        return self.URL_ROOT + self.link


season_table = Table("seasons",
                     MysqlBase.metadata,
                     Column('player_key', String(20), primary_key=True),
                     Column('season_start', Integer, primary_key=True, autoincrement=False),
                     Column('prior_season_start', Integer),
                     ForeignKeyConstraint(['player_key'], ['players.player_key']),
                     ForeignKeyConstraint(['player_key', 'prior_season_start'],
                                          ['seasons.player_key', 'seasons.season_start']))


class Season(MysqlBase):
    __table__ = season_table

    shooting = relationship("Shooting", cascade='all')
    pbp = relationship("PlayByPlay", cascade='all')
    totals = relationship("Totals", cascade='all')

    prior_season = relationship("Season",
                                backref='next_season',
                                remote_side=[season_table.columns['player_key'], season_table.columns['season_start']])

    @hybrid_property
    def totals_count(self):
        return len([row for row in self.totals])

    @totals_count.expression
    def totals_count(cls):
        alias = aliased(Totals)
        return select([func.count(alias.season_start)]). \
            where(and_(alias.player_key == cls.player_key,
                       alias.season_start == cls.season_start)).label('totals_count')

    @hybrid_property
    def mid_season_team_switch(self):
        return self.totals_count > 1


# mapper(Season, season_table)

shooting_table = Table("shooting",
                       MysqlBase.metadata,
                       Column('id', Integer, primary_key=True, autoincrement=True),
                       Column('player_key', String(20)),
                       Column('season', Text),
                       Column('season_start', Integer),
                       Column('age', Integer),
                       Column('team', String(90), index=True),
                       Column('league', Text),
                       Column('pos', Text),
                       Column('games', Integer),
                       Column('minutes', Integer),
                       Column('fg_pc', Float(8)),
                       Column('avg_dist', Float(8)),
                       Column('pc_fga_2p', Float(8)),
                       Column('pc_2pfga_0_3', Float(8)),
                       Column('pc_2pfga_3_10', Float(8)),
                       Column('pc_2pfga_10_16', Float(8)),
                       Column('pc_2pfga_16plus', Float(8)),
                       Column('pc_fga_3p', Float(8)),
                       Column('fgpc_2p', Float(8)),
                       Column('fgpc_2p_0_3', Float(8)),
                       Column('fgpc_2p_3_10', Float(8)),
                       Column('fgpc_2p_10_16', Float(8)),
                       Column('fgpc_2p_16plus', Float(8)),
                       Column('fgpc_3p', Float(8)),
                       Column('pc_2pfg_assisted', Float(8)),
                       Column('pc_fga_dunks', Float(8)),
                       Column('n_dunks', Integer),
                       Column('pc_3pfg_assisted', Float(8)),
                       Column('pc_3pfga_corner', Float(8)),
                       Column('fgpc_3pfga_corner', Float(8)),
                       Column('heaves_att', Integer),
                       Column('heaves_made', Integer),
                       ForeignKeyConstraint(['player_key', 'season_start'],
                                            ['seasons.player_key', 'seasons.season_start'])
)


class Shooting(MysqlBase):
    __table__ = shooting_table


pbp_table = Table('playbyplay',
                  MysqlBase.metadata,
                  Column('id', Integer, primary_key=True, autoincrement=True),
                  Column('player_key', String(20)),
                  Column('season', Text),
                  Column('season_start', Integer),
                  Column('age', Integer),
                  Column('team', String(90), index=True),
                  Column('league', Text),
                  Column('pos', Text),
                  Column('games', Integer),
                  Column('minutes', Integer),
                  Column('pos_pg_pc', Float(8)),
                  Column('pos_sg_pc', Float(8)),
                  Column('pos_sf_pc', Float(8)),
                  Column('pos_pf_pc', Float(8)),
                  Column('pos_c_pc', Float(8)),
                  Column('plus_minus_per_100_on_court', Float(8)),
                  Column('plus_minus_per_100_off_court', Float(8)),
                  Column('tos_off_foul', Integer),
                  Column('tos_bad_pass', Integer),
                  Column('tos_lost_ball', Integer),
                  Column('tos_other', Integer),
                  Column('points_generated_assist', Integer),
                  Column('and1', Integer),
                  Column('sfdrawn', Integer),
                  Column('fga_blocked', Integer),
                  ForeignKeyConstraint(['player_key', 'season_start'],
                                       ['seasons.player_key', 'seasons.season_start'])
)


class PlayByPlay(MysqlBase):
    __table__ = pbp_table


totals_table = Table('totals',
                     MysqlBase.metadata,
                     Column('id', Integer, primary_key=True, autoincrement=True),
                     Column('player_key', String(20)),
                     Column('season', Text),
                     Column('season_start', Integer),
                     Column('age', Integer),
                     Column('team', String(90), index=True),
                     Column('league', Text),
                     Column('pos', Text),
                     Column('games', Integer),
                     Column('games_started', Integer),
                     Column('minutes', Integer),
                     Column('fg', Integer),
                     Column('fga', Integer),
                     Column('fgpc', Float(8)),
                     Column('fg3p', Integer),
                     Column('fga3p', Integer),
                     Column('fgpc_3p', Float(8)),
                     Column('fg2p', Integer),
                     Column('fga2p', Integer),
                     Column('fgpc_2p', Float(8)),
                     Column('ft', Integer),
                     Column('fta', Integer),
                     Column('ftpc', Float(8)),
                     Column('orb', Integer),
                     Column('drb', Integer),
                     Column('trb', Integer),
                     Column('ast', Integer),
                     Column('stl', Integer),
                     Column('blk', Integer),
                     Column('tov', Integer),
                     Column('pf', Integer),
                     Column('pts', Integer),
                     ForeignKeyConstraint(['player_key', 'season_start'],
                                          ['seasons.player_key', 'seasons.season_start'])
)


class Totals(MysqlBase):
    __table__ = totals_table

