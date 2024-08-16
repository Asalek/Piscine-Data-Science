from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Float, BigInteger, MetaData, Table
import uuid
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import csv

db_name='piscineds'
db_pass='123456'
db_user='asalek'
db_host='127.0.0.1'

# # Define the SQLAlchemy model
Base = declarative_base()

path=f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

engine = create_engine(path)#, echo=True)


# class Customers(Base):
#     __tablename__ = "customers"

#     def __init__(self, first_name: str = "customers") -> None:
#         self.__tablename__ = first_name
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     event_time = Column(TIMESTAMP(timezone=True))
#     event_type = Column(String)
#     product_id = Column(Integer)
#     price = Column(Float)
#     user_id = Column(BigInteger)
#     user_session = Column(UUID(as_uuid=True))

#     def __init__(self, event_time, event_type, product_id, price, user_id, user_session):
#         self.event_time = event_time
#         self.event_type = event_type
#         self.product_id = product_id
#         self.price = price
#         self.user_id = user_id
#         self.user_session = user_session

# def new_func(Base, engine):
#     Base.metadata.create_all(engine)

# new_func(Base, engine)

metadata = MetaData()
data_2023_jan = Table('data_2023_jan', metadata, autoload_with=engine)
data_2022_oct = Table('data_2022_oct', metadata, autoload_with=engine)
data_2022_nov = Table('data_2022_nov', metadata, autoload_with=engine)
data_2022_dec = Table('data_2022_dec', metadata, autoload_with=engine)

Session = sessionmaker(bind=engine)
session = Session()

q = session.query(
    data_2022_oct.join(data_2023_jan)
    .join(data_2022_dec)
    .join(data_2022_nov)
)

# q = session.query(data_2022_oct).join(data_2023_jan, data_2022_oct.c.common_column == data_2023_jan.c.common_column) \
#     .join(data_2022_nov, data_2022_oct.c.common_column == data_2022_nov.c.common_column) \
#     .join(data_2022_dec, data_2022_oct.c.common_column == data_2022_dec.c.common_column)

# query = q.all()

query = session.query(q).all()
print(len(query))