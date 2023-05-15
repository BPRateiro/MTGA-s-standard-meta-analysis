import pandas as pd
from raw import request_analytics


def filter_raw_analytics(raw_analytics):
    # Reset index and rename tiers
    df = (raw_analytics.reset_index()
                       .rename(columns={'index': 'raw'}))

    # Split raw column into multiple columns
    columns = ['card_id', 'archetype_id', 'tier']
    df[columns] = df.raw.str.split('.', expand=True)

    # Record only consolidated data by archetype and change index to titleId
    df = (df[df.archetype_id == 'ALL'].drop(['raw', 'archetype_id'], axis=1)
                                      .set_index('card_id'))

    # Replace abbreviations with tier full names
    df.replace({'b': 'Bronze',
                's': 'Silver',
                'g': 'Gold',
                'p': 'Platinum'},
               inplace=True)

    # Unnest statistics
    unnest = ['games', 'wins', 'check', 'copies']
    df[unnest] = pd.DataFrame(df.explode([0])[0].to_list(),
                              index=df.index).iloc[:, :4]

    # Set index to integer
    df.index = df.index.astype('int64', copy=False)

    return df[['tier', 'wins', 'copies']]


def get_analytics_games(df):
    "Returns data frame 'analytics_games'"
    # Unnest copies
    unnest = [1, 2, 3, 4]
    df[unnest] = pd.DataFrame(df.copies.to_list(), index=df.index)

    df = (df.reset_index()
            .melt(id_vars=['card_id', 'tier'],
                  value_vars=unnest,
                  var_name='copies',
                  value_name='games')
            .set_index('card_id')
            .dropna())

    return df


def get_analytics(format_id):
    "Returns both 'analytics_games' and 'analytics_wins'"
    filtered = filter_raw_analytics(request_analytics(format_id))

    analytics_wins = filtered[['tier', 'wins']]
    analytics_games = get_analytics_games(filtered)

    return analytics_wins, analytics_games


if __name__ == '__main__':
    analytics_wins, analytics_games = get_analytics(str(364))

    print(analytics_wins.index.dtype)
    print(analytics_games.index.dtype)
