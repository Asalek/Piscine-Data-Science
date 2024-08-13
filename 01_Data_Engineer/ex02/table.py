from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Float, BigInteger
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
Session = sessionmaker(bind=engine)
session = Session()

#define class with a changed table_name
class data_2022_oct(Base):
    __tablename__ = "data_2022_oct_"

    def __init__(self, first_name: str = "data_2022_oct") -> None:
        self.__tablename__ = first_name
    id = Column(Integer, primary_key=True, autoincrement=True)
    event_time = Column(TIMESTAMP(timezone=True))
    event_type = Column(String)
    product_id = Column(Integer)
    price = Column(Float)
    user_id = Column(BigInteger)
    user_session = Column(UUID(as_uuid=True))

    def __init__(self, event_time, event_type, product_id, price, user_id, user_session):
        self.event_time = event_time
        self.event_type = event_type
        self.product_id = product_id
        self.price = price
        self.user_id = user_id
        self.user_session = user_session
    def printData(self):
        print(f"event_time: {self.event_time}\
              event_type: {self.event_type}\
                product_id: {self.product_id}\
                    price: {self.price}\
                        user_id: {self.user_id}\
                            user_session: {self.user_session}")

# Create the table
def new_func(Base, engine):
    Base.metadata.create_all(engine)

new_func(Base, engine)
 

def safe_uuid_conversion(uuid_string):
    try:
        return uuid.UUID(uuid_string)
    except ValueError:
        return None  # Or handle the error as needed

with open('/goinfre/asalek/subject/customer/data_2022_oct.csv') as file_obj: 
    i = 0
    reader_obj = csv.reader(file_obj)
    records = []
    for row in reader_obj:
        if i == 0:
            i += 1
            continue
        # Parse the row
        event_time = row[0]#datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S%z') # Adjust format as needed
        event_type = row[1]
        product_id = row[2]#int(row[2])
        price = row[3]#loat(row[3])
        user_id = row[4]#int(row[4])
        user_session = safe_uuid_conversion(row[5])#uuid.UUID(row[5])#row[5] if row[5] != '' else None#uuid.UUID(row[5])

        # Create a new instance of Data2022Oct
        new_record = data_2022_oct(event_time, event_type, product_id, price, user_id, user_session)
        records.append(new_record)

        if len(records) >= 1000:
            session.bulk_save_objects(records)#one query combine all data to be inserted
            session.commit()
            records = []

        # Add the record to the session
        # session.add(new_record)
        # session.commit()
        i += 1
    # Commit any remaining records
    if records:
        session.bulk_save_objects(records)
        session.commit()
    print(f"\033[92m{i-1}\033[0m row inserted successfully")
