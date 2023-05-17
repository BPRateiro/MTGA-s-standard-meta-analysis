import os

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, update, select
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from models import Set, AnalyticsWins, create_all
from analytics import get_analytics
from card import get_card_information
from raw import request_active

import pandas as pd


def get_engine():
    """Creates engine from URL, using environment variables"""
    url = URL.create(
        drivername="mysql+pymysql",
        username=os.getenv('UNTAPPED_USERNAME'),
        password=os.getenv('UNTAPPED_PASSWORD'),
        host=os.getenv('UNTAPPED_HOST'),
        port=os.getenv('UNTAPPED_PORT'),
        database="untapped"
    )
    return create_engine(url)


def garantee_database(eng):
    """Creates a database if one doesn't exist already"""
    if not database_exists(eng.url):
        create_database(eng.url)


def write_set(session):
    """Write set table"""
    # Get currently legal sets according to the website
    format, active_sets = request_active()

    # Get legal sets existing in the database
    existing_sets = (session.query(Set.id, Set.rotated_on)
                            .filter(Set.rotated_on.is_(None))
                            .all())
    existing_set_ids = {s[0] for s in existing_sets}

    # Get list of legal sets that are not in database
    new_sets = [s for s in active_sets if s not in existing_set_ids]

    # Write these new sets on the database
    if new_sets:
        new_entities = [Set(id=set_id) for set_id in new_sets]
        session.add_all(new_entities)
        session.commit()

    # Rotate sets that are not an active according to website
    existing_ids_to_update = [s[0] for s in existing_sets
                              if s[0] not in active_sets]
    if existing_ids_to_update:
        session.execute(
            update(Set)
            .where(Set.id.in_(existing_ids_to_update))
            .values(rotated_on=func.now())
        )
        session.commit()

    return format, new_sets


def write_card(connection):
    card_dataframes = get_card_information(request_active()[1])

    for card_df in card_dataframes:
        affected = card_df.to_sql(name=card_df.name,
                                  con=connection,
                                  if_exists='append')
        print(f"Table '{card_df.name}' had {affected} new inclusions")


def write_analytics(session, format):
    analytics_wins, analytics_games = get_analytics(format)

    # Write 'analytics_wins' DataFrame to the 'AnalyticsWins' table
    tablename = 'analytics_wins'
    affected = analytics_wins.to_sql(tablename,
                                     con=session.get_bind(),
                                     if_exists='append',
                                     index_label='card_id')
    print(f"Table '{tablename}' had {affected} new inclusions")

    # Retrieve records written on the previous step
    latest = session.query(func.max(AnalyticsWins.created_on)).scalar()
    query = (session.query(AnalyticsWins)
                    .filter(AnalyticsWins.created_on == latest))
    latest_records = pd.read_sql(query.statement, con=session.get_bind())

    # Merge the query result with 'analytics_games'
    merged_df = pd.merge(latest_records,
                         analytics_games,
                         on=['card_id', 'tier'])

    # Rename column 'id' to 'wins_id' in the merged DataFrame
    merged_df.rename(columns={'id': 'wins_id'}, inplace=True)

    # Write the relevant columns to 'AnalyticsGames' table
    tablename = 'analytics_games'
    columns = ['wins_id', 'copies', 'games']
    affected = merged_df[columns].to_sql(tablename,
                                         con=session.get_bind(),
                                         if_exists='append',
                                         index=False)
    print(f"Table '{tablename}' had {affected} new inclusions")


if __name__ == '__main__':
    engine = get_engine()  # Create engine
    garantee_database(engine)  # Make sure 'untapped' db exists

    with engine.connect() as connection:
        create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        # Write existing sets
        format, new_sets = write_set(session)
        print(f'The current format is {format}')

        # Include cards if there are new sets to include
        if new_sets:
            print(f'The following sets were included: {new_sets}')
            write_card(connection)
        else:
            print(f'No new sets were included')

        # Write analytics
        write_analytics(session, format)

        session.commit()
