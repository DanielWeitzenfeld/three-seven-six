import requests
from bs4 import BeautifulSoup
import pandas as pd


class table(object):
    def __init__(self, dom_id, columns, parse_dates=False, post_process=None):
        self.dom_id = dom_id
        self.columns = columns
        self.column_names = [c[0] for c in columns]
        self.parse_dates = parse_dates
        self.post_process = post_process

    def fetch_and_clean(self, url):
        df = pd.io.html.read_html(url, flavor='bs4', attrs={'id': self.dom_id}, parse_dates=self.parse_dates,
                                  infer_types=self.parse_dates,
                                  tupleize_cols=True, skiprows=0)
        import pdb; pdb.set_trace()
        df = df[0]
        df.columns = self.column_names + ['drop_%s' % i for i in range(len(df.columns) - len(self.column_names))]
        for c in [col for col in df.columns if 'drop_' in col]:
            df = df.drop(c, axis=1)
        for name, dtype in self.columns:
            df[name] = df[name].astype(dtype)
        if self.post_process:
            if isinstance(self.post_process, list):
                for p in self.post_process:
                    df = p(df)
            else:
                df = self.post_process(df)
        return df


def season_post_process(df):
    pattern = r"(?P<season_start>[0-9]+)(\-)(?P<season_end>[0-9]+)"
    df['season_start'] = df.season.str.extract(pattern)['season_start']
    df.season_start = df.season_start.astype(int)
    return df


SHOOTING = table('shooting', [
    ('season', object),
    ('age', int),
    ('team', object),
    ('league', object),
    ('pos', object),
    ('games', int),
    ('minutes', int),
    ('fg_pc', float),
    ('avg_dist', float),
    ('pc_fga_2p', float),
    ('pc_2pfga_0_3', float),
    ('pc_2pfga_3_10', float),
    ('pc_2pfga_10_16', float),
    ('pc_2pfga_16plus', float),
    ('pc_fga_3p', float),
    ('fgpc_2p', float),
    ('fgpc_2p_0_3', float),
    ('fgpc_2p_3_10', float),
    ('fgpc_2p_10_16', float),
    ('fgpc_2p_16plus', float),
    ('fgpc_3p', float),
    ('pc_2pfg_assisted', float),
    ('pc_fga_dunks', float),
    ('n_dunks', int),
    ('pc_3pfg_assisted', float),
    ('pc_3pfga_corner', float),
    ('fgpc_3pfga_corner', float),
    ('heaves_att', int),
    ('heaves_made', int),
], post_process=season_post_process)


def pos_pc_post_process(df):
    for c in ['pos_pg_pc',
              'pos_sg_pc',
              'pos_sf_pc',
              'pos_pf_pc',
              'pos_c_pc']:
        df[c] = df[c].str.replace('%', '')
        df[c] = df[c].astype(float)
        df[c] = df[c] / 100.0
    return df

PBP = table('advanced_pbp', [
    ('season', object),
    ('age', int),
    ('team', object),
    ('league', object),
    ('pos', object),
    ('games', int),
    ('minutes', int),
    ('pos_pg_pc', object),
    ('pos_sg_pc', object),
    ('pos_sf_pc', object),
    ('pos_pf_pc', object),
    ('pos_c_pc', object),
    ('plus_minus_per_100_on_court', float),
    ('plus_minus_per_100_off_court', float),
    ('tos_off_foul', int),
    ('tos_bad_pass', int),
    ('tos_lost_ball', int),
    ('tos_other', int),
    ('points_generated_assist', int),
    ('and1', int),
    ('sfdrawn', int),
    ('fga_blocked', int),
], post_process=[season_post_process, pos_pc_post_process])

TOTALS = table('totals', [
    ('season', object),
    ('age', int),
    ('team', object),
    ('league', object),
    ('pos', object),
    ('games', int),
    ('games_started', int),
    ('minutes', int),
    ('fg', int),
    ('fga', int),
    ('fgpc', float),
    ('fg3p', int),
    ('fga3p', int),
    ('fgpc_3p', float),
    ('fg2p', int),
    ('fga2p', int),
    ('fgpc_2p', float),
    ('ft', int),
    ('fta', int),
    ('ftpc', float),
    ('orb', int),
    ('drb', int),
    ('trb', int),
    ('ast', int),
    ('stl', int),
    ('blk', int),
    ('tov', int),
    ('pf', int),
    ('pts', int),
], post_process=season_post_process)


def players_post_process(df):
    df = df.rename(columns={'from': 'from_year',  # pandas can't handle a column with the name from
                            'to': 'to_year'})
    df.player = df.player.str.replace('*', '')  # kareem has an asterisk.
    pattern = r"(?P<feet>[0-9])(\-)(?P<inches>[0-9]+)"
    df[['height_feet', 'height_inches']] = df.height.str.extract(pattern)[['feet', 'inches']]
    df.height_feet = df.height_feet.astype(int)
    df.height_inches = df.height_inches.astype(int)
    df = df.drop('height', axis=1)
    df['height'] = df.height_feet * 12 + df.height_inches
    df = df.drop('height_feet', axis=1)
    df = df.drop('height_inches', axis=1)
    df.birth_date = pd.to_datetime(df.birth_date)
    return df


PLAYERS = table('players', [
    ('player', object),
    ('from', int),
    ('to', int),
    ('pos', object),
    ('height', object),
    ('weight', int),
    ('birth_date', object),
    ('college', object)
], post_process=players_post_process)


def scrape_hrefs_from_player_table(url, id='players'):
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    t = soup.find(id=id)
    assert t.contents[5].name == 'tbody'
    rows = t.contents[5].contents
    parsed_data = []
    for r in rows:
        if r.name != 'tr':  # not a real row
            continue
        name_cell = r.contents[1]
        name = name_cell.get_text()
        try:
            link = name_cell.contents[0]['href']
        except:
            link = name_cell.contents[0].contents[0]['href']
        parsed_data.append({'player': name, 'link': link})
    df = pd.DataFrame(parsed_data)
    pattern = r"(?P<crap>.+/)(?P<key>[a-z0-9]+)(?P<suffix>.+)"
    df['key'] = df.link.str.extract(pattern)['key']
    df.player = df.player.str.replace('*', '')
    return df


