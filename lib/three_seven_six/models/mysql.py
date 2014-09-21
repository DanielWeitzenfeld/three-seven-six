from __future__ import division
import datetime
from dateutil.relativedelta import relativedelta

from sqlalchemy import func, select, and_, or_, text, Text, Column, Integer, ForeignKey, String, Table, DateTime, Date, \
    Numeric, Boolean, cast, Float
from sqlalchemy import Column, Integer, ForeignKey, String, Table
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from sqlalchemy.sql.expression import case

import three_seven_six
from three_seven_six.dbs import mysql
from .mysql_base import MysqlBase


class Player(MysqlBase):
    __tablename__ = "players"
    id = Column(Integer, primary_key=True, autoincrement=True)
    player = Column(Text)
    key = Column(String(20), index=True)
    from_year = Column(Integer)
    to_year = Column(Integer)
    pos = Column(Text)
    weight = Column(Integer)
    birth_date = Column(Date)
    college = Column(Text)
    height = Column(Integer)
    link = Column(Text)
    scraped = Column(Boolean)

    shooting = relationship("Shooting", cascade="all")
    pbp = relationship("PlayByPlay", cascade="all")
    totals = relationship("Totals", cascade="all")

    URL_ROOT = 'http://www.basketball-reference.com'

    @hybrid_property
    def url(self):
        return self.URL_ROOT + self.link


class Shooting(MysqlBase):
    __tablename__ = "shooting"
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(20), ForeignKey('players.key'))
    season = Column(Text)
    season_start = Column(Integer, index=True)
    age = Column(Integer)
    team = Column(String(50), index=True)
    league = Column(Text)
    pos = Column(Text)
    games = Column(Integer)
    minutes = Column(Integer)
    fg_pc = Column(Float(8))
    avg_dist = Column(Float(8))
    pc_fga_2p = Column(Float(8))
    pc_2pfga_0_3 = Column(Float(8))
    pc_2pfga_3_10 = Column(Float(8))
    pc_2pfga_10_16 = Column(Float(8))
    pc_2pfga_16plus = Column(Float(8))
    pc_fga_3p = Column(Float(8))
    fgpc_2p = Column(Float(8))
    fgpc_2p_0_3 = Column(Float(8))
    fgpc_2p_3_10 = Column(Float(8))
    fgpc_2p_10_16 = Column(Float(8))
    fgpc_2p_16plus = Column(Float(8))
    fgpc_3p = Column(Float(8))
    pc_2pfg_assisted = Column(Float(8))
    pc_fga_dunks = Column(Float(8))
    n_dunks = Column(Integer)
    pc_3pfg_assisted = Column(Float(8))
    pc_3pfga_corner = Column(Float(8))
    fgpc_3pfga_corner = Column(Float(8))
    heaves_att = Column(Integer)
    heaves_made = Column(Integer)


class PlayByPlay(MysqlBase):
    __tablename__ = "playbyplay"
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(20), ForeignKey('players.key'))
    season = Column(Text)
    season_start = Column(Integer, index=True)
    age = Column(Integer)
    team = Column(String(50), index=True)
    league = Column(Text)
    pos = Column(Text)
    games = Column(Integer)
    minutes = Column(Integer)
    pos_pg_pc = Column(Float(8))
    pos_sg_pc = Column(Float(8))
    pos_sf_pc = Column(Float(8))
    pos_pf_pc = Column(Float(8))
    pos_c_pc = Column(Float(8))
    plus_minus_per_100_on_court = Column(Float(8))
    plus_minus_per_100_off_court = Column(Float(8))
    tos_off_foul = Column(Integer)
    tos_bad_pass = Column(Integer)
    tos_lost_ball = Column(Integer)
    tos_other = Column(Integer)
    points_generated_assist = Column(Integer)
    and1 = Column(Integer)
    sfdrawn = Column(Integer)
    fga_blocked = Column(Integer)


class Totals(MysqlBase):
    __tablename__ = "totals"
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(20), ForeignKey('players.key'))
    season = Column(Text)
    season_start = Column(Integer, index=True)
    age = Column(Integer)
    team = Column(String(50), index=True)
    league = Column(Text)
    pos = Column(Text)
    games = Column(Integer)
    games_started = Column(Integer)
    minutes = Column(Integer)
    fg = Column(Integer)
    fga = Column(Integer)
    fgpc = Column(Float(8))
    fg3p = Column(Integer)
    fga3p = Column(Integer)
    fgpc_3p = Column(Float(8))
    fg2p = Column(Integer)
    fga2p = Column(Integer)
    fgpc_2p = Column(Float(8))
    ft = Column(Integer)
    fta = Column(Integer)
    ftpc = Column(Float(8))
    orb = Column(Integer)
    drb = Column(Integer)
    trb = Column(Integer)
    ast = Column(Integer)
    stl = Column(Integer)
    blk = Column(Integer)
    tov = Column(Integer)
    pf = Column(Integer)
    pts = Column(Integer)



