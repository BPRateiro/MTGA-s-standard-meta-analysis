"""Requests JSONs and their raw dataframes equivalents"""

from functools import lru_cache
from time import sleep
import logging
import requests
import pandas as pd


# Relevant data is found in two different URLs
URLS = {
    "api": "https://api.mtga.untapped.gg/api/v1/",
    "json": "https://mtgajson.untapped.gg/v1/latest/",
}

# We read four JSONs in total. The following dictionary facilitates
# access to its endpoints.
# json: (url, endpoint)
ENDPOINTS = {
    "active": ("api", "meta-periods/active"),
    "analytics": (
        "api",
        "analytics/query/"
        "card_stats_by_archetype_event_and_scope_free/"
        "ALL?MetaPeriodId=",
    ),
    "cards": ("json", "cards.json"),
    "text": ("json", "loc_en.json"),
}

# Header informat_idion
HEADERS = {
    "authority": "api.mtga.untapped.gg",
    "accept": "*/*",
    "accept-language": "en-US,en;q=0.9,pt;q=0.8",
    "if-none-match": '"047066ff947f01e9e609ca4cf0d6c0a6"',
    "origin": "https://mtga.untapped.gg",
    "referer": "https://mtga.untapped.gg/",
    "sec-ch-ua": ('"Google Chrome";v="111", "Not(A:Brand";v="8",' '"Chromium";v="111"'),
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/"
        "537.36 (KHTML, like Gecko) Chrome/"
        "111.0.0.0 Safari/537.36"
    ),
}


@lru_cache
def request(keyword, send_headers=True, format_id=""):
    """Returns JSON from the corresponding keyword"""
    url_kw, endpoint = ENDPOINTS[keyword]
    url = URLS[url_kw]

    try:
        sleep(2)  # Resonable interval betwween requests

        if send_headers:
            response = requests.get(url + endpoint + format_id, HEADERS, timeout=30)
        else:
            response = requests.get(url + endpoint + format_id, timeout=30)

    except requests.exceptions.RequestException:
        logging.exception("An error occurred while requesting JSON from URL: %s", url)

    else:
        return response.json()


def request_active():
    """Returns format_id ID and lists of standard legal sets"""

    # Extracting informat_idion from the latest standard BO1 format_id
    for format_id in reversed(request("active")):
        if format_id["event_name"] == "Ladder":
            return str(format_id["id"]), format_id["legal_sets"]


def request_cards(sets):
    """Returns raw card data frame"""
    raw_card = pd.DataFrame(request("cards"))

    # Ony bother with standard legal cards
    raw_card = raw_card[raw_card.set.isin(sets)]

    # Remove duplicates by considering only the latest reprint
    latest_reprint = raw_card.groupby("titleId").agg({"grpid": "max"})
    raw_card = raw_card[raw_card.grpid.isin(latest_reprint.grpid)]

    return raw_card.set_index("grpid")


def request_analytics(format_id):
    """Returns raw analytics data frame"""
    json = request("analytics", False, format_id)

    return pd.json_normalize(json["data"]).T


def request_text():
    """Returns raw card text data frame"""
    raw_text = pd.DataFrame(request("text")).set_index("id")

    # Collapse columns raw and text, prioritizing raw
    raw_text.loc[~raw_text.raw.isna(), "text"] = raw_text.raw
    raw_text.drop("raw", axis="columns", inplace=True)

    return raw_text


if __name__ == "__main__":
    f_id, legal_sets = request_active()
    print(f_id, legal_sets)

    print("\n", request_cards(legal_sets))
    print("\n", request_analytics(f_id))
    print("\n", request_text())
