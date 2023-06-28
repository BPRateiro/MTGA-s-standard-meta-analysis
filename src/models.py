"""Defines untapped database tables through ORM"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Text, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()

# pylint: disable=E1102


class Set(Base):
    """Set table"""

    __tablename__ = "set"

    id = Column(String(3), primary_key=True)
    created_on = Column(DateTime, server_default=func.now())
    rotated_on = Column(DateTime, server_default=None, nullable=True)

    cards = relationship("Card", backref="set")


class Card(Base):
    """Card table"""

    __tablename__ = "card"

    id = Column(Integer, primary_key=True)
    art_id = Column(Integer)
    set_id = Column(String(3), ForeignKey("set.id"))
    title = Column(String(100), nullable=False, unique=True)
    rarity = Column(Text)
    power = Column(Text)
    toughness = Column(Text)
    flavor = Column(Text)
    is_legendary = Column(Boolean)
    is_token = Column(Boolean)
    is_secondary_card = Column(Boolean)
    is_rebalanced = Column(Boolean)
    created_on = Column(DateTime, server_default=func.now())

    types = relationship("CardType", backref="card")
    subtypes = relationship("CardSubtype", backref="card")
    costs = relationship("CardCost", backref="card")
    abilities = relationship("CardAbility", backref="card")
    games = relationship("AnalyticsGames", backref="card")


class CardType(Base):
    """Card type table"""

    __tablename__ = "card_type"

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey("card.id"))
    type = Column(Text)
    created_on = Column(DateTime, server_default=func.now())


class CardSubtype(Base):
    """Card subtype table"""

    __tablename__ = "card_subtype"

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey("card.id"))
    subtype = Column(Text)
    created_on = Column(DateTime, server_default=func.now())


class CardCost(Base):
    """Card cost table"""

    __tablename__ = "card_cost"

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey("card.id"))
    color = Column(Text)
    cost = Column(Integer)
    created_on = Column(DateTime, server_default=func.now())


class CardAbility(Base):
    """Card ability table"""

    __tablename__ = "card_ability"

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey("card.id"))
    ability = Column(Text)
    created_on = Column(DateTime, server_default=func.now())


class DistinctGames(Base):
    """Distinct games table"""

    __tablename__ = "distinct_games"

    id = Column(Integer, primary_key=True)
    tier = Column(String(10))
    total = Column(Integer)
    created_on = Column(DateTime, server_default=func.now())

    an_games = relationship("AnalyticsGames", backref="distinct_games")


class AnalyticsGames(Base):
    """Analytics games table"""

    __tablename__ = "analytics_games"

    id = Column(Integer, primary_key=True, autoincrement=True)
    card_id = Column(Integer, ForeignKey("card.id"), nullable=False)
    tier = Column(String(10))
    distinct_id = Column(Integer, ForeignKey("distinct_games.id"), nullable=False)
    games = Column(Integer)
    wins = Column(Integer)
    created_on = Column(DateTime, server_default=func.now())

    a_distribution = relationship("AnalyticsDistribution", backref="analytics_games")


class AnalyticsDistribution(Base):
    """Analytics distribution table"""

    __tablename__ = "analytics_distribution"

    id = Column(Integer, primary_key=True, autoincrement=True)
    games_id = Column(Integer, ForeignKey("analytics_games.id"), nullable=False)
    copies = Column(Integer)
    played = Column(Integer)
    created_on = Column(DateTime, server_default=func.now())


def drop_all(engine):
    """Drop all tables related to connected engine"""
    Base.metadata.drop_all(engine)


def create_all(engine):
    """Create all tables given connected engine"""
    Base.metadata.create_all(engine)


if __name__ == "__main__":
    print(Card.__table__)
