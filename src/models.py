from sqlalchemy import (Column, Integer, String,
                        Boolean, DateTime, Text, ForeignKey)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class Set(Base):
    __tablename__ = 'set'

    id = Column(String(3), primary_key=True)
    created_on = Column(DateTime, server_default=func.now())
    rotated_on = Column(DateTime, server_default=None, nullable=True)

    cards = relationship('Card', backref='set')


class Card(Base):
    __tablename__ = 'card'

    id = Column(Integer, primary_key=True)
    art_id = Column(Integer)
    set_id = Column(String(3), ForeignKey('set.id'))
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

    types = relationship('CardType', backref='card')
    subtypes = relationship('CardSubtype', backref='card')
    costs = relationship('CardCost', backref='card')
    abilities = relationship('CardAbility', backref='card')
    wins = relationship('AnalyticsWins', backref='card')


class CardType(Base):
    __tablename__ = 'card_type'

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey('card.id'))
    type = Column(Text)
    created_on = Column(DateTime, server_default=func.now())


class CardSubtype(Base):
    __tablename__ = 'card_subtype'

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey('card.id'))
    subtype = Column(Text)
    created_on = Column(DateTime, server_default=func.now())


class CardCost(Base):
    __tablename__ = 'card_cost'

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey('card.id'))
    color = Column(Text)
    cost = Column(Integer)
    created_on = Column(DateTime, server_default=func.now())


class CardAbility(Base):
    __tablename__ = 'card_ability'

    id = Column(Integer, primary_key=True)
    card_id = Column(Integer, ForeignKey('card.id'))
    ability = Column(Text)
    created_on = Column(DateTime, server_default=func.now())


class AnalyticsWins(Base):
    __tablename__ = 'analytics_wins'

    id = Column(Integer, primary_key=True, autoincrement=True)
    card_id = Column(Integer, ForeignKey('card.id'), nullable=False)
    tier = Column(String(10))
    wins = Column(Integer)
    created_on = Column(DateTime, server_default=func.now())

    games = relationship('AnalyticsGames', backref='wins')


class AnalyticsGames(Base):
    __tablename__ = 'analytics_games'

    id = Column(Integer, primary_key=True, autoincrement=True)
    wins_id = Column(Integer, ForeignKey('analytics_wins.id'), nullable=False)
    copies = Column(Integer)
    games = Column(Integer)
    created_on = Column(DateTime, server_default=func.now())


def drop_all(engine):
    Base.metadata.drop_all(engine)


def create_all(engine):
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    print(Card.__table__)
