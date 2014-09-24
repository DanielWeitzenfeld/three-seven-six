import os

original_startup = os.environ.get('THREE_SEVEN_SIX_ORIGINAL_PYTHONSTARTUP')
if original_startup:
    execfile(original_startup)

import warnings

warnings.filterwarnings('ignore')

import logging
from pprint import pprint

import pandas as pd
from datetime import datetime

import three_seven_six
from three_seven_six.models import *
from three_seven_six.dbs import mysql
from three_seven_six import bbalref_scraper
from sqlalchemy.orm import contains_eager, joinedload, subqueryload

session = mysql.Session()

logging.basicConfig()
log = logging.getLogger('sqlalchemy.engine')
log.setLevel(logging.INFO)


def turn_on_log():
    global log
    log.setLevel(logging.INFO)


def turn_off_log():
    global log
    log.setLevel(logging.WARN)


def fetch_player(player):
    p = session.query(Player).filter(Player.player == player).one()
    return p
