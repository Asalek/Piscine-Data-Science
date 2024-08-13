from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, BigInteger
from sqlalchemy.ext.declarative import declarative_base
import csv

db_name='piscineds'
db_pass='123456'
db_user='asalek'
db_host='127.0.0.1'

# # Define the SQLAlchemy model
Base = declarative_base()

path=f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

engine = create_engine(path)#, echo=True)
Session = sessionmaker(bind=engine)
session = Session()

#define class with a changed table_name
class items(Base):
    __tablename__ = "items"

    id = Column(Integer, primary_key=True, autoincrement=True)
    product_id = Column(Integer)
    category_id = Column(BigInteger)
    category_code = Column(String)
    brand = Column(String)

    def __init__(self, product_id, category_id, category_code, brand):
        self.product_id = product_id
        self.category_id = category_id
        self.category_code = category_code
        self.brand = brand

# Create the table
def new_func(Base, engine):
    Base.metadata.create_all(engine)

new_func(Base, engine)

with open('/goinfre/asalek/subject/item/item.csv') as file_obj: 
    i = 0
    reader_obj = csv.reader(file_obj)
    records = []
    for row in reader_obj:
        if i == 0:
            i += 1
            continue

        product_id = row[0] if row[0] != '' else None
        category_id = row[1] if row[1] != '' else None
        category_code = row[2] if row[2] != '' else None
        brand = row[3] if row[3] != '' else None
        
        new_record = items(product_id, category_id, category_code, brand)
        records.append(new_record)

        if len(records) >= 10000:
            session.bulk_save_objects(records)
            session.commit()
            records = []
        i += 1
    if records:
        session.bulk_save_objects(records)
        session.commit()
    print(f"\033[92m{i-1}\033[0m row inserted successfully")
