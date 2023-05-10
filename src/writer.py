from functools import lru_cache
import requests
import pandas as pd
import numpy as np
from time import sleep

class Writer:
    # Relevant data is found in two different URLs
    urls = {
        'api': 'https://api.mtga.untapped.gg/api/v1/',
        'json': 'https://mtgajson.untapped.gg/v1/latest/',
    }

    # We read four JSONs in total. The following dictionary facilitates access to its endpoints.
    # json: (url, endpoint) 
    endpoints = {
        'active': ('api', 'meta-periods/active'),
        'analytics': ('api', 'analytics/query/card_stats_by_archetype_event_and_scope_free/ALL?MetaPeriodId='),
        'cards': ('json', 'cards.json'),
        'text': ('json', 'loc_en.json'),
    }

    # Header information
    headers = {
        'authority': 'api.mtga.untapped.gg',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,pt;q=0.8',
        'if-none-match': '"047066ff947f01e9e609ca4cf0d6c0a6"',
        'origin': 'https://mtga.untapped.gg',
        'referer': 'https://mtga.untapped.gg/',
        'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    }

    @lru_cache
    def request(self, keyword, send_headers=True, format=''):
        'Returns JSON from the corresponding keyword'
        url_kw, endpoint = self.endpoints[keyword]
        url = self.urls[url_kw]

        sleep(2) # Resonable interval betwween requests

        try:
            if send_headers:
                response = requests.get(url+endpoint+format, self.headers)
            else:
                response = requests.get(url+endpoint+format)

        except requests.exceptions.RequestException as error:
            raise error
        
        else:
            return response.json()
        
    def request_active(self):
        'Requests format ID and list of standard legal sets'

        # Extracting information from the latest standard BO1 format
        for format in reversed(self.request('active')):
            if format['event_name'] == 'Ladder':
                self.format_id, self.legal_sets = str(format['id']), format['legal_sets']

    def request_cards(self):
        'Requests raw card data frame'
        df = pd.DataFrame(self.request('cards'))

        # Ony bother with standard legal cards
        df = df[df.set.isin(self.legal_sets)]

        # Remove duplicates by considering only the latest reprint
        gb = df.groupby('titleId').agg({'grpid':'max'})
        df = df[df.grpid.isin(gb.grpid)]

        self.raw_card = df.set_index('grpid')

    def request_analytics(self):
        'Returns raw analytics data frame'
        json = self.request('analytics', False, self.format_id)

        self.raw_analytics =  pd.json_normalize(json['data']).T

    def request_text(self):
        'Returns raw card text data frame'
        df = pd.DataFrame(self.request('text')).set_index('id')

        # Collapse columns raw and text, prioritizing raw
        df.loc[~df.raw.isna(), 'text'] = df.raw
        df.drop('raw', axis='columns', inplace=True)

        self.raw_text = df
    
    def filter_raw_card(self):
        "Change naming conventions and keep only relevant columns before normalization"

        keep = [
            'titleId',
            'art_id',
            'flavorId',
            'power',
            'toughness',
            'set_id',
            'castingcost',
            'rarity',
            'cardTypeTextId',
            'subtypeTextId',
            'ability',
            'is_secondary_card',
            'is_token',
            'is_rebalanced'
        ]

        rename = {
            'set':'set_id',
            'isSecondaryCard': 'is_secondary_card',
            'isToken': 'is_token',
            'IsRebalanced': 'is_rebalanced',
            'artId': 'art_id',
            'abilities': 'ability'
        }

        return self.raw_card.rename(rename, axis = 'columns').reset_index()[keep]
    
    def get_card_dataframe(self, df):
        "Returns card dataframe"   

        # Card columns mappable to text
        id_to_text = ['titleId', 'flavorId', 'cardTypeTextId', 'subtypeTextId']

        # Additional text_columns columns
        df = df.join(
            df[id_to_text]
            .applymap(lambda x: self.raw_text.loc[x].values[0], na_action='ignore')
            .rename(columns={column: column[:-2] for column in id_to_text}) # Remove Id
        )

        # Only tracked supertype will be 'legendary'
        df['is_legendary'] = df.cardTypeText.str.contains('Legendary')

        # Replace empty string flavor with NaN
        df.flavor = df.flavor.replace({'': np.nan})

        # Replace rarity id with text
        df.rarity = df.rarity.replace ({1: 'Common',
                                        2: 'Common',
                                        3: 'Uncommon',
                                        4: 'Rare',
                                        5: 'Mythic Rare'})

        # Define the columns and order for card data frame
        order = [
            'titleId',
            'art_id',
            'set_id',
            'title',
            'rarity',
            'power',
            'toughness',
            'flavor',
            'is_legendary',
            'is_token',
            'is_secondary_card',
            'is_rebalanced',
        ]

        return df[order]
    
    def get_card_type(self, df):
        'Returns card type data frame'
        # Convert 'cardTypeTextId' to text
        df = df.join(df.cardTypeTextId
                    .map(lambda x: self.raw_text.loc[x].values[0], na_action='ignore')
                    .rename('type'))
        
        # Split the text and transform it into a list of rows, deleting the ones without information
        df = df.type.str.split().explode().dropna()

        # There are special cases of cards not having types such as cards 'Day' and 'Night'
        df = df[~df.isin(['NONE', 'Legendary', 'Basic', 'Token'])]

        return df
    
    def get_card_subtype(self, df):
        "Returns card subtype data frame"
        # Convert 'subtypeTextId' to text
        df = df.join(df.subtypeTextId
                    .map(lambda x: self.raw_text.loc[x].values[0], na_action='ignore')
                    .rename('subtype'))

        # Split the text and transform it into a list of rows, deleting the ones without information
        df = df.subtype.str.split().explode().dropna()
        
        return df
    
    def get_card_cost(self, df):
        "Returns card cost data frame"
        # Casting color codes in 'castingcost' column
        casting_colors = {
            'Black': r'oB',
            'Blue': r'oU',
            'Green': r'oG',
            'Red': r'oR',
            'White': r'oW',
            'Multicolor': r'\(',
            'X': r'oX'
        }

        # Create a column for each variety
        for color, code in casting_colors.items():
            df[color] = df.castingcost.str.count(code)

        # Special case is colorless that can be any number
        df['Colorless'] = df.castingcost.str.extract(r'(\d+)')

        # Stack columns into rows and set index to card_id only
        df = (df[['Colorless'] + list(casting_colors.keys())].stack()
                                                            .reset_index()
                                                            .set_index('card_id')
                                                            .rename(columns={'level_1': 'color', 0: 'cost'}))

        df.cost = pd.to_numeric(df.cost) # Convert from string to number

        # Only record costs above zero
        return df[df.cost > 0]
    
    def get_card_ability(self, df):
        'Returns card_ability data frame'
        df = (df.ability
                .dropna()
                .explode()
                .apply(lambda x: x.get('TextId'))
                .map(lambda x: self.raw_text.loc[x].values[0]))      
        return df
    
    def get_card_information(self):
        'Returns multiple data frames containing card information after normalization'
        filtered = self.filter_raw_card()
        self.card = self.get_card_dataframe(filtered)

        # Rename 'titleId' to 'card_id' and promote it to index
        for df in [filtered, self.card]:
            df.set_index('titleId', inplace=True)
            df.index.name = 'card_id'

        self.card_type = self.get_card_type(filtered)
        self.card_subtype = self.get_card_subtype(filtered)
        self.card_cost = self.get_card_cost(filtered)
        self.card_ability = self.get_card_ability(filtered)

    def filter_raw_analytics(self):
        # Reset index and rename tiers
        df = (self.raw_analytics.reset_index()
                                .rename(columns={'index':'raw'}))
        
        # Split raw column into multiple
        df[['card_id', 'archetype_id', 'tier']] = df.raw.str.split('.', expand=True)

        # Record only consolidated data by archetype and change index to titleId
        df = (df[df.archetype_id == 'ALL']
                .drop(['raw', 'archetype_id'], axis=1)
                .set_index('card_id'))
        
        # Replace abbreviations with tier full names
        df.replace({'b': 'Bronze',
                    's': 'Silver',
                    'g': 'Gold',
                    'p': 'Platinum'},
                inplace = True)

        # Unnest statistics
        unnest = ['games', 'wins', 'check', 'copies']
        df[unnest] = pd.DataFrame(df.explode([0])[0].to_list(), index=df.index).iloc[:, :4]

        return df[['tier', 'wins', 'copies']]
    
    def get_card_tiered_daily_games(self, df):
        "Returns data frame 'analytics_games'"
        # Unnest copies
        unnest = [1, 2, 3, 4]
        df[unnest] = pd.DataFrame(df.copies.to_list(), index=df.index)

        df = (df.reset_index()
                .melt(id_vars = ['card_id', 'tier'],
                    value_vars = unnest,
                    var_name = 'copies',
                    value_name = 'games')
                .set_index('card_id')
                .dropna())
        
        return df
    
    def get_analytics(self):
        "Returns both 'analytics_games' and 'analytics_wins'"
        filtered = self.filter_raw_analytics()

        self.analytics_wins = filtered[['tier', 'wins']]
        self.analytics_games = self.get_card_tiered_daily_games(filtered)

    def get_all(self):
        self.request_active()
        self.request_cards()
        self.request_analytics()
        self.request_text()
        self.get_card_information()
        self.get_analytics()

if __name__ == '__main__':
    writer = Writer()
    writer.get_all()
    print(writer.card_type)
