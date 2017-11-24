from sqlalchemy import create_engine
from sqlalchemy import schema, types
from sqlalchemy import Table,Column,Integer,String,MetaData
from sqlalchemy import text
from sqlalchemy import inspect
from sqlalchemy import select

"""
-------------------------------------------
IF OBJECT_ID(N'dbo.Cars',N'u') is not null 
DROP TABLE dbo.Cars
GO
CREATE TABLE Cars(Id INTEGER PRIMARY KEY, Name TEXT, Price INTEGER);
INSERT INTO Cars VALUES(1, 'Audi', 52642);
INSERT INTO Cars VALUES(2, 'Mercedes', 57127);
INSERT INTO Cars VALUES(3, 'Skoda', 9000);
INSERT INTO Cars VALUES(4, 'Volvo', 29000);
INSERT INTO Cars VALUES(5, 'Bentley', 350000);
INSERT INTO Cars VALUES(6, 'Citroen', 21000);
INSERT INTO Cars VALUES(7, 'Hummer', 41400);
INSERT INTO Cars VALUES(8, 'Volkswagen', 21600)
-------------------------------------------
"""
"""
-------------------------------------------
IF OBJECT_ID(N'dbo.Authors',N'u') is not null 
DROP TABLE dbo.Authors
GO
CREATE TABLE Authors(AuthorId INTEGER PRIMARY KEY, Name TEXT);
INSERT INTO Authors VALUES(1, 'Jane Austen');
INSERT INTO Authors VALUES(2, 'Leo Tolstoy');
INSERT INTO Authors VALUES(3, 'Joseph Heller');
INSERT INTO Authors VALUES(4, 'Charles Dickens')
"""

"""
--------------------------------------------
CREATE TABLE Books(BookId INTEGER PRIMARY KEY, Title TEXT, AuthorId INTEGER, 
FOREIGN KEY(AuthorId) REFERENCES Authors(AuthorId));
INSERT INTO Books VALUES(1,'Emma',1);
INSERT INTO Books VALUES(2,'War and Peace',2);
INSERT INTO Books VALUES(3,'Catch XII',3);
INSERT INTO Books VALUES(4,'David Copperfield',4);
INSERT INTO Books VALUES(5,'Good as Gold',3);
INSERT INTO Books VALUES(6,'Anna Karenia',2);
"""

#############################################RAW SQL AND CONNECTIONS#############################################
#connection to database
engine = create_engine('mssql+pyodbc://DOM/Northwind?driver=SQL Server Native Client 11.0')

#connector - virtual database
connection=engine.connect()

#drop table
connection.execute(text("IF OBJECT_ID(N'dbo.Cars',N'u') is not null DROP TABLE dbo.Cars"))

#create table
connection.execute(text("CREATE TABLE Cars(Id INTEGER PRIMARY KEY, Name TEXT, Price INTEGER)"))

#using of dictionary to insert data into table

data=({"Id": 1 ,"Name": "Audi" ,"Price": 52642},
      {"Id":2, "Name":"Mercedes", "Price":57127},
      {"Id":3, "Name":"Skoda","Price":9000},
      {"Id":4,"Name":"Volvo","Price":29000},
      {"Id":5,"Name":"Bentley","Price":350000},
      {"Id":6,"Name":"Citroen","Price":21000},
      {"Id":7,"Name":"Hummer","Price":41400},
      {"Id":8,"Name":"Volkswagen","Price":21600}
      )

#inserting data to table
for line in data:
    connection.execute(text("INSERT INTO Cars(Id, Name, Price) VALUES(:Id, :Name, :Price)"),line)

#check table
CarsTable=connection.execute(text('SELECT * FROM Cars'))
#show's columns name in table
print(CarsTable.keys())
for row in CarsTable:
    print(row)


#############################################METADATA#############################################
"""
Metadata is information about the data in the database; for instance information about the tables and columns, in which we store data. 
"""

#itS neccessary to definition table, without it we dont create table
meta = MetaData()


#drop table
#connection.execute(text("IF OBJECT_ID(N'dbo.Authors',N'u') IS NOT NULL DROP TABLE dbo.Authors"))

#second approach to create table, first is execute with raw SQL statement, here we use functions from SQLAlchemy
#but now theres tables are only created in python cache no in SQL Server
authors = Table('Authors',meta,
                Column('Id',Integer,primary_key=True),
                Column('Name',String)
                )
books = Table('Books',meta,
              Column('Id',Integer,primary_key=True),
              Column('Title',String),
              Column('AuthorId',Integer)
              )
"""
authorsDictionary = ({'Id':1,'Name':'Szczepan Twardoch'},
                     {'Id':2, 'Name':'Adam Mickiewicz'},
                     {'Id':3, 'Name':'Dan Brown'},
                     {'Id':4, 'Name':'Agata Christie'},
                     {'Id':5, 'Name':'Stephan King'},
                     {'Id':6, 'Name':'Michaił Bułhakov'}
                     )
authors.insert(values=authorsDictionary)
"""

"""
print("Print all columns in Authors")
for columns in authors.c:
    print(columns.name,' ',columns.type)

print("\nPrint all columns in Books")
for columns in books.c:
    print(columns.name, ' ', columns.type)

print("\nPrint Primary_Key in Authors")
for columns in authors.primary_key:
    print(columns.name, ' ', columns.type)

print("\nPrint Primary_Key in Books")
for columns in books.primary_key:
    print(columns.name, ' ', columns.type)
"""
#method reflect()
#here we related reflect method with our engine - and or engice is simply connection do conrete database
#the most important is bind=engine, because without it we print nothing!!! it's a connector for this
meta.reflect(bind=engine)

#it's allow to see all tables in database
for table in meta.tables:
    print("Table name:",table)

#method inspect()
#it's also important connection
#we can check all informations about tables and columns - MetaData
inspector = inspect(engine)
print(inspector.get_table_names())
for table in inspector.get_table_names():
    print(inspector.get_columns(table))

#############################################EXPRESSIONS#############################################

#####SELECTS#####

sdEngine = create_engine('mssql+pyodbc://DOM/Northwind?driver=SQL Server Native Client 11.0')

sdConnection=sdEngine.connect()
with sdConnection as conn:
    sdMeta=MetaData(sdEngine)
    #in this case, we provide to function Table exisiting table in SQL server. We recive variable with data and we make select on Variable
    sdCars=Table('Cars',sdMeta,autoload=True)
                 #Column('Id',Integer,primary_key=True),
                 #Column('Name',String),
                 #Column('Price',Integer)
                 #)
    stmSelect=select([sdCars])
    results=conn.execute(stmSelect)
    print(results.fetchall())
    conn.close()

sdConnection=sdEngine.connect()
with sdConnection as conn:
    sdMeta=MetaData(sdEngine)
    #definition of table
    sdCategories=Table('Categories',sdMeta,autoload=True)
    stmSelect=select([sdCategories])
    results=conn.execute(stmSelect)
    for row in results:
        print(row)
    conn.close()

sdConnection=sdEngine.connect()
with sdConnection as conn:
    sdMeta=MetaData(sdEngine)
    sdCategories=Table('Categories',sdMeta,autoload=True)
    #Check columns names in table
    #for column in sdCategories.c:
    #    print(column.name)
    stmSelect=select([sdCategories.c.CategoryName,sdCategories.c.Description])
    results=conn.execute(stmSelect)
    print(results.keys())
    for row in results:
        print(row)
    conn.close()

