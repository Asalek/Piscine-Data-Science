# Python to create a table in PostgreSql 

Create a postgres table using the data from a CSV from the ’customer’ folder.
Name the tables according to the CSV’s name but without the file extension, for
example : "data_2022_oct"
• The name of the columns must be the same as the one in the CSV files and have
the appropriate type, beware you should have at least 6 different data types
• A DATETIME as the first column is mandatory
Be careful, the typings are not quite the same as under Maria DB

## Installed Packages

* pip install SQLAlchemy
* pip install psycopg2-binary

in case of any error happen in installation use thies pips:

* pip install --only-binary :all: greenlet

if didn't work use this instead:
* pip install --only-binary :all: Flask-SQLAlchemy


then instal sqlAlchemy again
