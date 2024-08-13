from sqlalchemy import create_engine, Column, Integer, String, TIMESTAMP, Float, BigInteger
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import uuid
import csv
import os

# Database connection details
db_name = 'piscineds'
db_pass = '123456'
db_user = 'asalek'
db_host = '127.0.0.1'

# Database URI
path = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

# Create SQLAlchemy engine and session
engine = create_engine(path)  # , echo=True)
Session = sessionmaker(bind=engine)
session = Session()

# Base class for SQLAlchemy models
Base = declarative_base()

# Helper function for safe UUID conversion
def safe_uuid_conversion(uuid_string):
    try:
        return uuid.UUID(uuid_string)
    except ValueError:
        return None  # Or handle the error as needed

# Dictionary to track created tables
created_tables = {}

# Function to create a new table class dynamically
def create_table_class(table_name):
    if table_name in created_tables:
        return created_tables[table_name]

    # Define a new class with the given table name
    class DynamicTable(Base):
        __tablename__ = table_name
        id = Column(Integer, primary_key=True, autoincrement=True)
        event_time = Column(TIMESTAMP(timezone=True))
        event_type = Column(String)
        product_id = Column(Integer)
        price = Column(Float)
        user_id = Column(BigInteger)
        user_session = Column(UUID(as_uuid=True))

    # Add the new class to the Base metadata
    Base.metadata.create_all(engine, tables=[DynamicTable.__table__])
    
    created_tables[table_name] = DynamicTable
    return DynamicTable

# Function to create tables for files
def create_tables_for_files(file_list):
    for file in file_list:
        table_name = file.split('.')[0]
        create_table_class(table_name)

# Directory containing CSV files
path = "/goinfre/asalek/subject/customer"
dir_list = os.listdir(path)

# Create tables for each file
create_tables_for_files(dir_list)

# Insert data into each table
for file in dir_list:
    table_name = file.split('.')[0]
    DynamicTable = create_table_class(table_name)

    with open(f'/goinfre/asalek/subject/customer/{file}') as file_obj:
        i = 0
        reader_obj = csv.reader(file_obj)
        records = []
        for row in reader_obj:
            if i == 0:
                i += 1
                continue
            event_time = row[0]
            event_type = row[1]
            product_id = row[2]
            price = row[3]
            user_id = row[4]
            user_session = safe_uuid_conversion(row[5])

            # Create a new instance of the dynamic table class
            new_record = DynamicTable(
                event_time=event_time,
                event_type=event_type,
                product_id=product_id,
                price=price,
                user_id=user_id,
                user_session=user_session
            )
            records.append(new_record)

            if len(records) >= 1000:
                session.bulk_save_objects(records)
                session.commit()
                records = []

            i += 1

        # Commit any remaining records
        if records:
            session.bulk_save_objects(records)
            session.commit()
        print(f"\033[92m{i-1}\033[0m rows inserted successfully into {table_name}")
session.close()
