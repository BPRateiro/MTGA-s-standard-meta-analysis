from functools import lru_cache
from time import sleep
import requests
import logging
import pandas as pd


# Relevant data is found in two different URLs
URLS = {
    'api': 'https://api.mtga.untapped.gg/api/v1/',
    'json': 'https://mtgajson.untapped.gg/v1/latest/',
}

# We read four JSONs in total. The following dictionary facilitates
# access to its endpoints.
# json: (url, endpoint)
ENDPOINTS = {
    'active': ('api', 'meta-periods/active'),
    'analytics': ('api',
                  'analytics/query/'
                  'card_stats_by_archetype_event_and_scope_free/'
                  'ALL?MetaPeriodId='),
    'cards': ('json', 'cards.json'),
    'text': ('json', 'loc_en.json'),
}

# Header information
HEADERS = {
    'authority': 'api.mtga.untapped.gg',
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,pt;q=0.8',
    'if-none-match': '"047066ff947f01e9e609ca4cf0d6c0a6"',
    'origin': 'https://mtga.untapped.gg',
    'referer': 'https://mtga.untapped.gg/',
    'sec-ch-ua': ('"Google Chrome";v="111", "Not(A:Brand";v="8",'
                  '"Chromium";v="111"'),
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/'
                   '537.36 (KHTML, like Gecko) Chrome/'
                   '111.0.0.0 Safari/537.36'),
}


@lru_cache
def request(keyword, send_headers=True, format=''):
    """Returns JSON from the corresponding keyword"""
    url_kw, endpoint = ENDPOINTS[keyword]
    url = URLS[url_kw]

    try:
        sleep(2)  # Resonable interval betwween requests

        if send_headers:
            response = requests.get(url+endpoint+format, HEADERS)
        else:
            response = requests.get(url+endpoint+format)

    except requests.exceptions.RequestException as error:
        logging.exception(
            f"An error occurred while requesting JSON from URL: {url}")

    else:
        return response.json()


def request_active():
    """Returns format ID and lists of standard legal sets"""

    # Extracting information from the latest standard BO1 format
    for format in reversed(request('active')):
        if format['event_name'] == 'Ladder':
            return str(format['id']), format['legal_sets']


def request_cards(sets):
    """Returns raw card data frame"""
    df = pd.DataFrame(request('cards'))

    # Ony bother with standard legal cards
    df = df[df.set.isin(sets)]

    # Remove duplicates by considering only the latest reprint
    gb = df.groupby('titleId').agg({'grpid': 'max'})
    df = df[df.grpid.isin(gb.grpid)]

    return df.set_index('grpid')


def request_analytics(format):
    """Returns raw analytics data frame"""
    json = request('analytics', False, format)

    return pd.json_normalize(json['data']).T


def request_text():
    """Returns raw card text data frame"""
    df = pd.DataFrame(request('text')).set_index('id')

    # Collapse columns raw and text, prioritizing raw
    df.loc[~df.raw.isna(), 'text'] = df.raw
    df.drop('raw', axis='columns', inplace=True)

    return df


if __name__ == '__main__':
    format_id, legal_sets = request_active()
    print(format_id, legal_sets)

    print('\n', request_cards(legal_sets))
    print('\n', request_analytics(format_id))
    print('\n', request_text())
