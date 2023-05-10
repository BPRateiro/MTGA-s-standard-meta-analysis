"""Creates database and tables according to predetermined schema"""

import os

from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

from models import drop_all, create_all
import writer as wr

def main():
    """ Creates database and tables"""
    url = URL.create(
        drivername = "mysql+pymysql",
        username = os.getenv('UNTAPPED_USERNAME'),
        password = os.getenv('UNTAPPED_PASSWORD'),
        host = os.getenv('UNTAPPED_HOST'),
        port = os.getenv('UNTAPPED_PORT'),
        database = "untapped"
    )

    engine = create_engine(url)

    # Garantee that a database with certain name exists
    if not database_exists(engine.url):
        create_database(engine.url)

    with engine.connect() as connection:
        drop_all(engine) # For testing only
        create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        writer = wr.Writer()
        writer.get_all()

        writer.card.to_sql(
            name = 'card',
            con = engine,
            if_exists = 'append',
            index = True,
            index_label = 'id'
        )

        session.commit()

if __name__ == '__main__':
    main()
