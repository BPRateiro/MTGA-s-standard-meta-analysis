"""Card information related dataframes to be written in the database"""

import pandas as pd
import numpy as np

from raw import request_cards, request_text, request_active


def filter_raw_card(raw_card):
    """
    Change naming conventions and keep only
    relevant columns before normalization
    """

    keep = [
        "titleId",
        "grpid",
        "flavorId",
        "power",
        "toughness",
        "set_id",
        "castingcost",
        "rarity",
        "cardTypeTextId",
        "subtypeTextId",
        "ability",
        "is_secondary_card",
        "is_token",
        "is_rebalanced",
    ]

    rename = {
        "set": "set_id",
        "isSecondaryCard": "is_secondary_card",
        "isToken": "is_token",
        "IsRebalanced": "is_rebalanced",
        "abilities": "ability",
    }

    return raw_card.rename(rename, axis="columns").reset_index()[keep]


def get_card_dataframe(filtered_df, raw_text):
    """Returns card dataframe"""

    # Card columns mappable to text
    id_to_text = ["titleId", "flavorId", "cardTypeTextId", "subtypeTextId"]

    # Additional text_columns columns
    filtered_df = filtered_df.join(
        filtered_df[id_to_text]
        .applymap(lambda x: raw_text.loc[x].values[0], na_action="ignore")
        .rename(columns={column: column[:-2] for column in id_to_text})
    )

    # Only tracked supertype will be 'legendary'
    filtered_df["is_legendary"] = filtered_df.cardTypeText.str.contains("Legendary")

    # Replace empty string flavor with NaN
    filtered_df.flavor = filtered_df.flavor.replace({"": np.nan})

    # Replace rarity id with text
    filtered_df.rarity = filtered_df.rarity.replace(
        {1: "Common", 2: "Common", 3: "Uncommon", 4: "Rare", 5: "Mythic Rare"}
    )

    # Fill NaN for boolean columns
    boolean_columns = ["is_token", "is_secondary_card", "is_rebalanced"]
    filtered_df[boolean_columns] = filtered_df[boolean_columns].fillna(False)

    # Generate art link from set and grpid
    filtered_df["art_link"] = (
        "https://mtgajson.untapped.gg/art/full_grpid/en/512/"
        + filtered_df.set_id.str.lower()
        + "/"
        + filtered_df.grpid.astype(str)
        + ".webp"
    )

    # Define the columns and order for card data frame
    order = [
        "titleId",
        "art_link",
        "set_id",
        "title",
        "rarity",
        "power",
        "toughness",
        "flavor",
        "is_legendary",
        "is_token",
        "is_secondary_card",
        "is_rebalanced",
    ]

    return filtered_df[order]


def get_card_type(filtered_df, raw_text):
    """Returns card type data frame"""
    # Convert 'cardTypeTextId' to text
    filtered_df = filtered_df.join(
        filtered_df.cardTypeTextId.map(
            lambda x: raw_text.loc[x].values[0], na_action="ignore"
        ).rename("type")
    )

    # Split the text and transform it into a list of rows,
    # deleting the ones without information
    filtered_df = filtered_df.type.str.split().explode().dropna()

    # There are special cases of cards not having types
    # such as cards 'Day' and 'Night'
    filtered_df = filtered_df[
        ~filtered_df.isin(["NONE", "Legendary", "Basic", "Token"])
    ]

    return filtered_df.to_frame()


def get_card_subtype(filtered_df, raw_text):
    """Returns card subtype data frame"""
    # Convert 'subtypeTextId' to text
    filtered_df = filtered_df.join(
        filtered_df.subtypeTextId.map(
            lambda x: (raw_text.loc[x].values[0]), na_action="ignore"
        ).rename("subtype")
    )

    # Split the text and transform it into a list of rows,
    # deleting the ones without information
    filtered_df = filtered_df.subtype.str.split().explode().dropna()

    return filtered_df.to_frame()


def get_card_cost(filtered_df):
    """Returns card cost data frame"""
    # Casting color codes in 'castingcost' column
    casting_colors = {
        "Black": r"oB",
        "Blue": r"oU",
        "Green": r"oG",
        "Red": r"oR",
        "White": r"oW",
        "Multicolor": r"\(",
        "X": r"oX",
    }

    # Create a column for each variety
    for color, code in casting_colors.items():
        filtered_df[color] = filtered_df.castingcost.str.count(code)

    # Special case is colorless that can be any number
    filtered_df["Colorless"] = filtered_df.castingcost.str.extract(r"(\d+)")

    # Stack columns into rows and set index to card_id only
    columns = ["Colorless"] + list(casting_colors.keys())
    filtered_df = (
        filtered_df[columns]
        .stack()
        .reset_index()
        .set_index("card_id")
        .rename(columns={"level_1": "color", 0: "cost"})
    )

    filtered_df.cost = pd.to_numeric(filtered_df.cost)  # Convert from string to number

    # Only record costs above zero
    return filtered_df[filtered_df.cost > 0]


def get_card_ability(filtered_df, raw_text):
    """Returns card_ability data frame"""
    filtered_df = (
        filtered_df.ability.dropna()
        .explode()
        .apply(lambda x: x.get("TextId"))
        .map(lambda x: raw_text.loc[x].values[0])
    )
    return filtered_df.to_frame()


def get_card_information(sets):
    """
    Returns multiple data frames containing card
    information after normalization
    """
    raw_card = request_cards(sets)
    raw_text = request_text()

    filtered = filter_raw_card(raw_card)
    card = get_card_dataframe(filtered, raw_text)

    # Rename 'titleId' to 'card_id' and promote it to index
    for dataframe in [filtered, card]:
        dataframe.set_index("titleId", inplace=True)

    # Rename 'titleId' to correct nomenclature on schema
    filtered.index.name = "card_id"
    card.index.name = "id"

    card_type = get_card_type(filtered, raw_text)
    card_subtype = get_card_subtype(filtered, raw_text)
    card_cost = get_card_cost(filtered)
    card_ability = get_card_ability(filtered, raw_text)

    # Set names:
    card.name = "card"
    card_type.name = "card_type"
    card_subtype.name = "card_subtype"
    card_cost.name = "card_cost"
    card_ability.name = "card_ability"

    return card, card_type, card_subtype, card_cost, card_ability


if __name__ == "__main__":
    (
        card_df,
        card_type_df,
        card_subtype_df,
        card_cost_df,
        card_ability_df,
    ) = get_card_information(request_active()[1])

    print(card_df)
    print("\n", card_type_df)
    print("\n", card_subtype_df)
    print("\n", card_cost_df)
    print("\n", card_ability_df)
