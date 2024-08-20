from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text, Column, Integer, String, TIMESTAMP, Float, BigInteger,func, update
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.functions import coalesce

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
    category_id = Column(BigInteger)
    category_code = Column(String)
    brand = Column(String)

class items(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer)
    category_id = Column(BigInteger)
    category_code = Column(String)
    brand = Column(String)

session.execute(text('ALTER TABLE customers\
                    ADD COLUMN IF NOT EXISTS category_id BIGINT,\
                    ADD COLUMN IF NOT EXISTS category_code VARCHAR(1000),\
                    ADD COLUMN IF NOT EXISTS brand VARCHAR(1000)\
                '))
session.commit()

q = session.query(
    items.product_id.label("product_id"),
    coalesce(func.max(items.category_code)).label("category_code"),
    coalesce(func.max(items.brand)).label("brand"),
    coalesce(func.max(items.category_id)).label("category_id")
).group_by(items.product_id).subquery()

stmt = (
    update(Customers)
    .values(
        brand=q.c.brand,
        category_code=q.c.category_code,
        category_id=q.c.category_id
    )
    .where(Customers.product_id == q.c.product_id)
)

session.execute(stmt)
session.commit()

# q2 = session.query(Customers).join(
#             q, Customers.product_id == q.c.product_id
#         ).update(
#             {
#                 Customers.category_code: q.c.category_code,
#                 Customers.brand: q.c.brand,
#                 Customers.category_id: q.c.category_id
#             },
#         )

# with engine.connect() as connection:
#     sql = connection.execute(text('select * from items'))
#     for x in sql:
#       print(x)

