import requests
from bs4 import BeautifulSoup
import pandas as pd

SHOOTING_TABLE_ID = 'shooting'

SHOOTING_TABLE_COLUMNS = [
    'season',
    'age',
    'team',
    'league',
    'pos',
    'games',
    'minutes',
    'fg_pc',
    'avg_dist',
    'pc_fga_2p',
    'pc_2pfga_0_3',
    'pc_2pfga_3_10',
    'pc_2pfga_10_16',
    'pc_2pfga_16plus',
    'pc_fga_3p',
    'fgpc_2p',
    'fgpc_2p_0_3',
    'fgpc_2p_3_10',
    'fgpc_2p_10_16',
    'fgpc_2p_16plus',
    'fgpc_3p',
    'pc_2pfg_assisted',
    'pc_fga_dunks',
    'n_dunks',
    'pc_3pfg_assisted',
    'pc_3pfga_corner',
    'fgpc_3pfga_corner',
    'heaves_att',
    'heaves_made'
]

PBP_TABLE_ID = 'advanced_pbp'

PBP_TABLE_COLUMNS = [
    'season',
    'age',
    'team',
    'league',
    'pos',
    'games',
    'minutes_played',
    'pos_pg_pc',
    'pos_sg_pc',
    'pos_sf_pc',
    'pos_pf_pc',
    'pos_c_pc',
    'plus_minus_per_100_on_court',
    'plus_minus_per_100_off_court',
    'tos_off_foul',
    'tos_bad_pass',
    'tos_lost_ball',
    'tos_other',
    'points_generated_assist',
    'and1',
    'sfdrawn',
    'fga_blocked'
]

TOTALS_TABLE_ID = 'totals'

TOTALS_TABLE_COLUMNS = [
    'season',
    'age',
    'team',
    'league',
    'pos',
    'games',
    'games_started',
    'minutes_played',
    'fg',
    'fga',
    'fgpc',
    'fg3p',
    'fga3p',
    'fgpc_3p',
    'fg2p',
    'fga2p',
    'fgpc_2p',
    'ft',
    'fta',
    'ftpc',
    'orb',
    'drb',
    'trb',
    'ast',
    'stl',
    'blk',
    'tov',
    'pf',
    'pts'
]

PLAYERS_TABLE_ID = 'players'

PLAYERS_TABLE_COLUMNS = [
    'player',
    'from',
    'to',
    'pos',
    'height',
    'weight',
    'birth_date',
    'college'
]


def fetch_and_clean_table(url, id, column_set):
    df = pd.io.html.read_html(url, flavor='bs4', attrs={'id': id}, tupleize_cols=True, skiprows=0)
    df = df[0]
    df.columns = column_set + ['drop_%s' % i for i in range(len(df.columns) - len(column_set))]
    for c in [col for col in df.columns if 'drop_' in col]:
        df = df.drop(c, axis=1)
    return df


def scrape_hrefs_from_player_table(url, id='players'):
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    t = soup.find(id=id)
    assert t.contents[5].name == 'tbody'
    rows = t.contents[5].contents
    parsed_data = []
    for r in rows:
        if r.name != 'tr': #not a real row
            continue
        name_cell = r.contents[1]
        name = name_cell.get_text()
        try:
            link = name_cell.contents[0]['href']
        except:
            link = name_cell.contents[0].contents[0]['href']
        parsed_data.append({'name': name, 'link': link})
    return pd.DataFrame(parsed_data)


