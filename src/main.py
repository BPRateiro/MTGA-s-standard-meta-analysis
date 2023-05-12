import os

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, update
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

import pandas as pd

from models import Set, drop_all, create_all
from card import get_card_information
from raw import request_active


def start_engine():
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
    "Write set table"
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


if __name__ == '__main__':
    engine = start_engine()  # Create engine
    garantee_database(engine)  # Make sure 'untapped' db exists

    with engine.connect() as connection:
        # drop_all(engine)  # For testing only
        create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        # Write data
        format, new_sets = write_set(session)
        print(f'The current format is {format}')

        if new_sets:
            print(f'The following sets were included: {new_sets}')
            write_card(connection)
        else:
            print(f'No new sets were included')

        session.commit()
