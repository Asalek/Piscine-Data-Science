from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Float, BigInteger, MetaData, Table
from sqlalchemy.dialects.postgresql import UUID, insert
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import select, union_all

db_name = 'piscineds'
db_pass = '123456'
db_user = 'asalek'
db_host = '127.0.0.1'

# Define the SQLAlchemy model
Base = declarative_base()

path = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

engine = create_engine(path)#, echo=True)


class Customers(Base):
    __tablename__ = "customers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_time = Column(TIMESTAMP(timezone=True))
    event_type = Column(String)
    product_id = Column(Integer)
    price = Column(Float)
    user_id = Column(BigInteger)
    user_session = Column(UUID(as_uuid=True))

# Create tables if they don't exist
Base.metadata.create_all(engine)

# Load the metadata and define the existing tables
#metadata Used to reflect the existing database tables.
metadata = MetaData()
# autoload_with: allows SQLAlchemy to automatically load the table structure (columns, types)
data_2023_jan = Table('data_2023_jan', metadata, autoload_with=engine)
data_2022_oct = Table('data_2022_oct', metadata, autoload_with=engine)
data_2022_nov = Table('data_2022_nov', metadata, autoload_with=engine)
data_2022_dec = Table('data_2022_dec', metadata, autoload_with=engine)

Session = sessionmaker(bind=engine)
session = Session()

# Create the UNION ALL query without selecting the id column
union_query = union_all(
    select(
        data_2022_dec.c.event_time,
        data_2022_dec.c.event_type,
        data_2022_dec.c.product_id,
        data_2022_dec.c.price,
        data_2022_dec.c.user_id,
        data_2022_dec.c.user_session
    ),
    select(
        data_2022_nov.c.event_time,
        data_2022_nov.c.event_type,
        data_2022_nov.c.product_id,
        data_2022_nov.c.price,
        data_2022_nov.c.user_id,
        data_2022_nov.c.user_session
    ),
    select(
        data_2022_oct.c.event_time,
        data_2022_oct.c.event_type,
        data_2022_oct.c.product_id,
        data_2022_oct.c.price,
        data_2022_oct.c.user_id,
        data_2022_oct.c.user_session
    ),
    select(
        data_2023_jan.c.event_time,
        data_2023_jan.c.event_type,
        data_2023_jan.c.product_id,
        data_2023_jan.c.price,
        data_2023_jan.c.user_id,
        data_2023_jan.c.user_session
    )
)

# Create the Customers table using the UNION ALL query
#from_select specify the columns to be inserted into (insert into tableA (columns, ...))
stmt = insert(Customers).from_select(
    [
        Customers.event_time,
        Customers.event_type,
        Customers.product_id,
        Customers.price,
        Customers.user_id,
        Customers.user_session
    ],
    union_query
)


# Execute the statement and commit the transaction
session.execute(stmt)
session.commit()

# Close the session
session.close()
