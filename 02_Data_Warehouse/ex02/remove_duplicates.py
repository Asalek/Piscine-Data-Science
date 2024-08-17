from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Float, BigInteger, MetaData, Table, func, delete
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select

db_name = 'piscineds'
db_pass = '123456'
db_user = 'asalek'
db_host = '127.0.0.1'

# Define the SQLAlchemy model
Base = declarative_base()

path = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

engine = create_engine(path)#, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

class Customers(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_time = Column(TIMESTAMP(timezone=True))
    event_type = Column(String)
    product_id = Column(Integer)
    price = Column(Float)
    user_id = Column(BigInteger)
    user_session = Column(UUID(as_uuid=True))

subquery = (
    select(
        Customers,
        func.row_number()
        .over(
            partition_by=[
                Customers.event_type,
                Customers.price,
                Customers.product_id,
                Customers.user_id,
                Customers.user_session,
                func.date_trunc('day', Customers.event_time)
            ],
            order_by=Customers.id
        )
        .label("Row_Number")
    )
).alias("Duplicated_Rows")

# Select the rows where Row_Number > 1 (i.e., duplicates)
duplicated_rows_query = select(subquery).where(subquery.c.Row_Number > 1)

# Fetch and print the duplicated rows
duplicated_rows = session.execute(duplicated_rows_query).fetchall()

if duplicated_rows:
    print("Duplicated Rows:")
    for row in duplicated_rows:
        print(row)

# Optionally, delete the duplicated rows
duplicated_ids = [row[0] for row in duplicated_rows]  # Assuming the first column is the ID

if duplicated_ids:
    stmt = delete(Customers).where(Customers.id.in_(duplicated_ids))
    session.execute(stmt)
    session.commit()
