import time, getopt
import string
import sys
from dateutil.relativedelta import relativedelta

import pandas as pd

import three_seven_six
from three_seven_six import application, bbalref_scraper
from three_seven_six.dbs import mysql as mysql_db
from three_seven_six.models import *


WAIT_BETWEEN_REQUESTS = 3
PLAYERS_URL = 'http://www.basketball-reference.com/players/'


class App(application.Application):
    def app_name(self):
        return 'setup'

    def __init__(self, argslist):
        application.Application.__init__(self)
        args = ['recreate_player_queue=']
        opts, extraparams = getopt.getopt(argslist[1:], '', args)
        self.create_player_queue = False
        for o, p in opts:
            if o == '--recreate_player_queue':
                self.recreate_player_queue = bool(p)
        self.info('Called with recreate_player_queue=%s' % self.recreate_player_queue)

    def runApplication(self):
        MysqlBase.metadata.create_all(mysql_db.engine)
        if self.recreate_player_queue:
            self.create_player_table()


    def create_player_table(self):
        for letter in list(string.ascii_lowercase):
            url = PLAYERS_URL + letter + '/'
            df = bbalref_scraper.fetch_and_clean_table(url,
                                                       bbalref_scraper.PLAYERS_TABLE_ID,
                                                       bbalref_scraper.PLAYERS_TABLE_COLUMNS)




