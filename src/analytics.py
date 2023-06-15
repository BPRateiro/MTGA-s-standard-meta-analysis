"""Analytics dataframes to be written in the database"""

import pandas as pd

from raw import request_analytics


def filter_raw_analytics(raw_analytics):
    """Unnest the 'raw_analytics' columns and makes tier names more explicit"""
    # Reset index and rename tiers
    analytics_df = raw_analytics.reset_index().rename(columns={"index": "raw"})

    # Split raw column into multiple columns
    columns = ["card_id", "archetype_id", "tier"]
    analytics_df[columns] = analytics_df.raw.str.split(".", expand=True)

    # Record only consolidated data by archetype and change index to titleId
    analytics_df = (
        analytics_df[analytics_df.archetype_id == "ALL"]
        .drop(["raw", "archetype_id"], axis=1)
        .set_index("card_id")
    )

    # Replace abbreviations with tier full names
    analytics_df.replace(
        {"b": "Bronze", "s": "Silver", "g": "Gold", "p": "Platinum"}, inplace=True
    )

    # Unnest statistics
    unnest = ["games", "wins", "check", "copies"]
    analytics_df[unnest] = pd.DataFrame(
        analytics_df.explode([0])[0].to_list(), index=analytics_df.index
    ).iloc[:, :4]

    # Set index to integer
    analytics_df.index = analytics_df.index.astype("int64", copy=False)

    return analytics_df[["tier", "wins", "copies"]]


def get_analytics_games(filtered_df):
    """Returns data frame 'analytics_games'"""
    # Unnest copies
    unnest = [1, 2, 3, 4]
    filtered_df[unnest] = pd.DataFrame(
        filtered_df.copies.to_list(), index=filtered_df.index
    )

    filtered_df = (
        filtered_df.reset_index()
        .melt(
            id_vars=["card_id", "tier"],
            value_vars=unnest,
            var_name="copies",
            value_name="games",
        )
        .set_index("card_id")
        .dropna()
    )

    return filtered_df


def get_analytics(format_id):
    """Returns both 'analytics_games' and 'analytics_wins'"""
    filtered = filter_raw_analytics(request_analytics(format_id))

    analytics_wins = filtered[["tier", "wins"]]
    analytics_games = get_analytics_games(filtered)

    return analytics_wins, analytics_games


if __name__ == "__main__":
    aw, ag = get_analytics(str(364))

    print(aw.index.dtype)
    print(ag.index.dtype)
