"""Controls order of writing to the database operation"""

import os

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, update
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

import pandas as pd

from models import Set, AnalyticsGames, DistinctGames, create_all
from analytics import get_analytics
from card import get_card_information
from raw import request_active

# pylint: disable=E1102


def get_engine():
    """Creates engine from URL, using environment variables"""
    url = URL.create(
        drivername="mysql+pymysql",
        host=os.getenv("MYSQL_HOST"),
        username=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
    )
    return create_engine(url)


def garantee_database(eng):
    """Creates a database if one doesn't exist already"""
    if not database_exists(eng.url):
        create_database(eng.url)


def write_set(session):
    """Write set table"""
    # Get currently legal sets according to the website
    format_id, active_sets = request_active()

    # Get legal sets existing in the database
    existing_sets = (
        session.query(Set.id, Set.rotated_on).filter(Set.rotated_on.is_(None)).all()
    )
    existing_set_ids = {s[0] for s in existing_sets}

    # Get list of legal sets that are not in database
    new_sets = [s for s in active_sets if s not in existing_set_ids]

    # Write these new sets on the database
    if new_sets:
        new_entities = [Set(id=set_id) for set_id in new_sets]
        session.add_all(new_entities)
        session.commit()

    # Rotate sets that are not an active according to website
    existing_ids_to_update = [s[0] for s in existing_sets if s[0] not in active_sets]
    if existing_ids_to_update:
        session.execute(
            update(Set)
            .where(Set.id.in_(existing_ids_to_update))
            .values(rotated_on=func.now())
        )
        session.commit()

    return format_id, new_sets


def write_card(connection):
    """Writes all card related dataframes to database"""
    card_dataframes = get_card_information(request_active()[1])

    for card_df in card_dataframes:
        affected = card_df.to_sql(name=card_df.name, con=connection, if_exists="append")
        print(f"Table '{card_df.name}' had {affected} new inclusions")


def write_dataframe(session, tablename, dataframe, index=False, index_label=None):
    """Write generic analytics dataframe"""
    affected = dataframe.to_sql(
        tablename,
        con=session.get_bind(),
        if_exists="append",
        index=index,
        index_label=index_label,
    )
    session.commit()  # Garantee that database is ready to be queried
    print(f"Table '{tablename}' had {affected} new inclusions")


def retrieve_latest(session, table):
    """Retrieve records written on the previous step"""
    latest = session.query(func.max(table.created_on)).scalar()
    query = session.query(table).filter(table.created_on == latest)
    return pd.read_sql(query.statement, con=session.get_bind())


def merge_dataframe(left, right, merging_columns, renamed):
    """Merge generic dataframes"""
    merged = pd.merge(left, right, on=merging_columns)

    # Rename column 'id' to 'wins_id' in the merged DataFrame
    merged.rename(columns={"id": renamed}, inplace=True)

    return merged.sort_values(merging_columns)


def write_analytics(session, format_id):
    """Writes both analytics related dataframes to database"""
    distinct_games, analytics_games, analytics_distribution = get_analytics(format_id)

    # Write 'distinct_games' DataFrame to the 'DistinctGames' table
    write_dataframe(session, "distinct_games", distinct_games, True, "tier")

    # Retrieve records written on the previous step
    latest_records = retrieve_latest(session, DistinctGames)

    # Merge the query result with 'analytics_distribution'
    merged_df = merge_dataframe(
        analytics_games.reset_index(), latest_records, ["tier"], "distinct_id"
    )

    # Write merged_df DataFrame to the 'AnalyticsGames' table
    columns = ["card_id", "tier", "distinct_id", "games", "wins"]
    write_dataframe(
        session, "analytics_games", merged_df[columns].sort_values("card_id")
    )

    # Retrieve records written on the previous step
    latest_records = retrieve_latest(session, AnalyticsGames)

    # Merge the query result with 'analytics_distribution'
    merged_df = merge_dataframe(
        latest_records, analytics_distribution, ["card_id", "tier"], "games_id"
    )

    # Write the relevant columns to 'AnalyticsDistribution' table
    columns = ["games_id", "copies", "played"]
    write_dataframe(session, "analytics_distribution", merged_df[columns])


if __name__ == "__main__":
    engine = get_engine()  # Create engine
    garantee_database(engine)  # Make sure 'untapped' db exists

    with engine.connect() as open_connection:
        create_all(engine)

        Session = sessionmaker(bind=engine)
        open_session = Session()

        # Write existing sets
        current_format, included_sets = write_set(open_session)
        print(f"The current format is {current_format}")

        # Include cards if there are new sets to include
        if included_sets:
            print(f"The following sets were included: {included_sets}")
            write_card(open_connection)
        else:
            print("No new sets were included")

        # Write analytics
        write_analytics(open_session, current_format)

        open_session.commit()
