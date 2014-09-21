import time, getopt
import string
import sys
from dateutil.relativedelta import relativedelta

import pandas as pd

import three_seven_six
from three_seven_six import application, bbalref_scraper
from three_seven_six.dbs import mysql as mysql_db
from three_seven_six.models import *

WAIT_BETWEEN_REQUESTS = 2.6
PLAYERS_URL = 'http://www.basketball-reference.com/players/'
MUST_HAVE_PLAYED_SINCE = 2001

class App(application.Application):
    def app_name(self):
        return 'setup'

    def __init__(self, argslist):
        application.Application.__init__(self)
        args = ['from_scratch=']
        opts, extraparams = getopt.getopt(argslist[1:], '', args)
        self.from_scratch = False
        for o, p in opts:
            if o == '--from_scratch':
                self.from_scratch = bool(p)
        self.info('Called with from_scratch=%s' % self.from_scratch)

    def runApplication(self):
        MysqlBase.metadata.create_all(mysql_db.engine)
        self.mysql.flush()
        self.mysql.commit()
        if self.from_scratch:
            self.create_player_table()
        self.scrape_player_data()

    def create_player_table(self):
        mysql_db.execute(self.mysql, """delete from players where True;""")
        self.mysql.flush()
        self.mysql.commit()
        all_players = []
        for letter in list(string.ascii_lowercase):
            url = PLAYERS_URL + letter + '/'
            try:
                df = bbalref_scraper.PLAYERS.fetch_and_clean(url)
            except:
                self.info('Failed to find or parse table on page: %s' % url)
            time.sleep(WAIT_BETWEEN_REQUESTS)
            df_links = bbalref_scraper.scrape_hrefs_from_player_table(url,
                                                                      bbalref_scraper.PLAYERS.dom_id)
            df = pd.merge(df, df_links, on='player', how='outer')
            all_players.append(df)
            time.sleep(WAIT_BETWEEN_REQUESTS)
        df = pd.concat(all_players, ignore_index=True)
        df['scraped'] = False
        df2 = df.astype(object).where(pd.notnull(df), None)
        df2.to_sql('players', con=mysql_db.engine, if_exists='append', index=False)
        self.mysql.flush()
        self.mysql.commit()

    def scrape_player_data(self):
        players = self.mysql.query(Player).filter(Player.to_year > MUST_HAVE_PLAYED_SINCE,
                                                  Player.scraped == False).all()
        for p in players:
            self.info(p.player)
            try:
                df_shooting = bbalref_scraper.SHOOTING.fetch_and_clean(p.url)
                df_shooting['key'] = p.key
                df_shooting2 = df_shooting.astype(object).where(pd.notnull(df_shooting), None)
                df_shooting2.to_sql('shooting', con=mysql_db.engine, if_exists='append', index=False)
            except:
                self.info('shit the bed on shooting: %s, url: %s' % (p.player, p.url))
                import pdb; pdb.set_trace()

            time.sleep(WAIT_BETWEEN_REQUESTS)
            try:
                df_pbp = bbalref_scraper.PBP.fetch_and_clean(p.url)
                df_pbp['key'] = p.key
                df_pbp2 = df_pbp.astype(object).where(pd.notnull(df_pbp), None)
                df_pbp2.to_sql('playbyplay', con=mysql_db.engine, if_exists='append', index=False)
            except:
                self.info('shit the bed on pbp: %s, url: %s' % (p.player, p.url))
                import pdb; pdb.set_trace()

            time.sleep(WAIT_BETWEEN_REQUESTS)
            try:
                df_totals = bbalref_scraper.TOTALS.fetch_and_clean(p.url)
                df_totals['key'] = p.key
                df_totals2 = df_totals.astype(object).where(pd.notnull(df_totals), None)
                df_totals2.to_sql('totals', con=mysql_db.engine, if_exists='append', index=False)
            except:
                self.info('shit the bed on totals: %s, url: %s' % (p.player, p.url))
                import pdb; pdb.set_trace()

            p.scraped = True
            self.mysql.flush()
            self.mysql.commit()
            time.sleep(WAIT_BETWEEN_REQUESTS)


    def ensure_data_not_already_in_table(self, table, player_key):
        mysql_db.execute(self.mysql, """DELETE FROM %s where key = %s;""" % (table, player_key))

    def table_exists(self, table):
        results = mysql_db.execute(self.mysql, """SHOW TABLES LIKE '%s';""" % table, df=False)
        return True if results else False



