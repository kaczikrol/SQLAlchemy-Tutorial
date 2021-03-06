from sqlalchemy import create_engine
from sqlalchemy import schema, types
from sqlalchemy import Table,Column,Integer,String,MetaData
from sqlalchemy import text
from sqlalchemy import inspect
from sqlalchemy import select
from sqlalchemy import and_, or_
from sqlalchemy import asc,desc, tuple_
from sqlalchemy import ForeignKey
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
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
#Metadata is information about the data in the database; for instance information about the tables and columns, in which we store data.
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


#if we want select concrete column we must use variableTable.c.ColumnName in select -> select([ourStatement])
sdConnection=sdEngine.connect()
with sdConnection as conn:
    sdMeta=MetaData(sdEngine)
    sdCustomers=Table('Customers',sdMeta,autoload=True)
    stmSelect=select([sdCustomers.c.CompanyName,sdCustomers.c.City,sdCustomers.c.Country])
    results=conn.execute(stmSelect)
    print(results.keys())
    for row in results:
        print(row)
    conn.close()

#limit(), where() methods to filter data
#select([statement]).where(statement)
#select([statement]).limit(integer)


print('\n\n\n')
sdConnection=sdEngine.connect()
with sdConnection as conn:
    sdMeta=MetaData(sdEngine)
    sdCustomers=Table('Customers',sdMeta,autoload=True)
    #tips - please notice that we use here equal statement from python (==) not from SQL (=)
    PolandCustomers=select([sdCustomers.c.CompanyName,sdCustomers.c.City,sdCustomers.c.Country]).where(sdCustomers.c.Country=='Poland')
    GermanyCustomers=select([sdCustomers.c.CompanyName,sdCustomers.c.City,sdCustomers.c.Country]).where(sdCustomers.c.Country=='Germany')
    FranceCustomers=select([sdCustomers.c.CompanyName,sdCustomers.c.City,sdCustomers.c.Country]).where(sdCustomers.c.Country=='France')
    ItalyCustomers=select([sdCustomers.c.CompanyName, sdCustomers.c.City, sdCustomers.c.Country]).where(sdCustomers.c.Country == 'Italy')


    results=conn.execute(PolandCustomers)
    print("Poland Customers")
    print(results.keys())
    for row in results:
        print(row)

    print('\n\n\n')
    results=conn.execute(GermanyCustomers)
    print("Germany Customers")
    print(results.keys())
    for row in results:
        print(row)

    print('\n\n\n')
    results=conn.execute(FranceCustomers)
    print("France Customeres")
    print(results.keys())
    for row in results:
        print(row)

    print('\n\n\n')
    results=conn.execute(ItalyCustomers)
    print("Italy Customeres")
    print(results.keys())
    for row in results:
        print(row)

    conn.close()
"""
#like() method

#engine = create_engine('mssql+pyodbc://DOM/Northwind?driver=SQL Server Native Client 11.0')
#connection = engine.connect()

"""
with connection as conn:
    meta=MetaData(engine)
    Customers=Table('Customers',meta,autoload=True)
    stm=select([Customers])
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)
conn.close()
"""
"""
with connection as conn:
    meta=MetaData(engine)
    customers=Table('Customers',meta,autoload=True)
    stm=select([customers]).where(or_(customers.c.Country=='Poland',customers.c.Country=='Germany'))
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)
"""

###IMPROTANT - METHOD REFLECT ALLOW TO SEE ALL TABLES AND ALL COLUMNS IN DATABASE - REMEBER!!!!!!###
"""
with connection as conn:
    meta=MetaData(engine)
    #meta.reflect()
    #for tables in meta.tables:
    #    print(tables)
    orderDetails=Table('Order Details',meta,autoload=True)
    stm=select([orderDetails]).where(orderDetails.c.UnitPrice*orderDetails.c.Quantity>2000)
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)
"""

#method like()
# we use it in select([statement]).where(statement.like(assumptions))
"""
with connection as conn:
    meta=MetaData(engine)
    #meta.reflect()
    #for table in meta.tables:
    #    print(table)
    employees=Table('Employees',meta,autoload=True)
    stm=select([employees]).where(employees.c.FirstName.like('an%'))
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)
"""
#method order_by() its important to add to import desc or asc
"""
with connection as conn:
    meta=MetaData(engine)
    #reflect() to check list of tables available in database
    #meta.reflect()
    #for table in meta.tables:
    #    print(table)
    territories=Table('Territories',meta,autoload=True)
    stm=select([territories]).order_by(territories.c.TerritoryDescription)
    results=conn.execute(stm)
    print(results.keys())
    countRows=0
    for row in results:
        print(row)
        countRows+=1
    print("Number of rows: ",countRows)
"""
"""
#below example how to use order_by() method
#select([columns]).order_by(asc(column) or desc(columns))
with connection as conn:
    meta=MetaData(engine)
    meta.reflect()
    for table in meta.tables:
        print(table)
    products=Table('Products',meta,autoload=True)
    stm=select([products.c.ProductName,products.c.UnitPrice]).order_by(desc(products.c.UnitPrice))
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)
"""
#in_() operator and tuple_()

"""
with connection as conn:
    #in this case tuple (1,) it allows have a lot of arguments
    indicator=[(1,),(2,),(3,)]
    indicator2=[(1),(2),(3)]
    meta=MetaData(engine)
    meta.reflect()
    for table in meta.tables:
        print(table)
    employees=Table('Employees',meta,autoload=True)
    #first version
    #stm=select([employees]).where(tuple_(employees.c.EmployeeID).in_(indicator))
    stm=select([employees]).where(employees.c.EmployeeID.in_(indicator2))
    #second version
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)
"""

"""
with connection as conn:
    titleVariable=[('Sales Representative',)]
    meta=MetaData(engine)
    employees=Table('Employees',meta,autoload=True)
    stm=select([employees]).where(tuple_(employees.c.Title).in_(titleVariable))
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)
"""

###############################CREATING TABLES################################
"""

engine = create_engine('mssql+pyodbc://DOM/Northwind?driver=SQL Server Native Client 11.0')
connection = engine.connect()
#connection.execute(text("IF OBJECT_ID(N'dbo.Authors',N'u') IS NOT NULL DROP TABLE dbo.Authors"))
connection.execute(text("DROP TABLE dbo.Authors"))

with connection as conn:
    meta=MetaData(engine)
    authors=Table('Authors',meta,
                  Column('Id',Integer,primary_key=True),
                  Column('FirstName',String),
                  Column('LastName',String),
                  Column('BirthYear',Integer))
    authors.create()

#{'First Name':,'Last Name':,'Birth Year':}
    authorsdictionary=({'FirstName':'Przemek','LastName':'Kaczmarek','BirthYear':1993},
                       {'FirstName':'Michaił','LastName':'Bułhakov','BirthYear':1815},
                       {'FirstName':'Adam', 'LastName':'Mickiewicz', 'BirthYear':1785},
                       {'FirstName':'Ken', 'LastName':'Kessey', 'BirthYear':1942}
                       )

    insertdata=authors.insert().values(authorsdictionary)
    conn.execute(insertdata)

    stm=select([authors]).where(authors.c.BirthYear>1900)
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)
"""

###############################JOINING TABLES###############################

#join() method
"""
engine = create_engine('mssql+pyodbc://DOM/Northwind?driver=SQL Server Native Client 11.0')
connection = engine.connect()
#connection.execute(text("IF OBJECT_ID(N'dbo.Authors',N'u') is not null drop table dbo.Authors"))
#connection.execute(text("IF OBJECT_ID(N'dbo.Books',N'u') is not null drop table dbo.Books"))

connection.execute(text("drop table dbo.Books"))
connection.execute(text("drop table dbo.Authors"))



with connection as conn:
    meta=MetaData(engine)
    Authors=Table('Authors',meta,
                  Column('Id',Integer,primary_key=True),
                  Column('FirstName',String),
                  Column('LastName',String),
                  Column('BirthYear',Integer))
    Books=Table('Books',meta,
                Column('Id',Integer,primary_key=True),
                Column('Title',String),
                Column('RelaseYear',Integer),
                Column('AuthorId',Integer,ForeignKey("Authors.Id")))
    Authors.create()
    Books.create()
    #{'FirstName':, 'LastName':, 'BirthYear':}
    dataAuthors=({'FirstName':'Adam','LastName':'Mickiewicz','BirthYear':1798},
                 {'FirstName':'Szczepan', 'LastName':'Twardoch', 'BirthYear':1978},
                 {'FirstName':'Remigiusz', 'LastName':'Mróz', 'BirthYear':1984},
                 {'FirstName':'Zygmunt', 'LastName':'Miłoszewski', 'BirthYear':1975},
                 {'FirstName':'Przemysław', 'LastName':'Kaczmarek', 'BirthYear':1993})

    #{'Title':,'RelaseYear','AuthorId':}
    dataBooks=({'Title':'Pan Tadeusz','RelaseYear':1848,'AuthorId':1},
              {'Title':'Wotum nieufności', 'RelaseYear':2016, 'AuthorId':3},
              {'Title':'Ojciec Chrzestny', 'RelaseYear':1980, 'AuthorId':None},
              {'Title':'Bezcenny', 'RelaseYear':2011, 'AuthorId':4},
              {'Title':'Morfina', 'RelaseYear':2011, 'AuthorId':2},
              {'Title':'Ojciec Goriot', 'RelaseYear':1890, 'AuthorId':None},
              {'Title':'Drach', 'RelaseYear':2015, 'AuthorId':2})

    #IMPORTANT PATTERN TO INSERT DATA stmVariable=tableVariable.insert().values(dataVariable)
    insertAuthors=Authors.insert().values(dataAuthors)
    insertBooks=Books.insert().values(dataBooks)
    conn.execute(insertAuthors)
    conn.execute(insertBooks)


    ###join() method###
    stm=select([Books.join(Authors)])
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)

    ###outerjoin() method - it is left join###
    stm=select([Books.outerjoin(Authors)]).where(Books.c.RelaseYear>2013).order_by(Books.c.Title)
    results=conn.execute(stm)
    print(results.keys())
    for row in results:
        print(row)

"""
#Important question how I can make more than one query in one connection? Multi-threaded
#now I solved it with open and close new connections but it's stupid! Search a better solution!!!

###############################OBJECT RELATIONAL MAPPING###############################
engine = create_engine('mssql+pyodbc://DOM/Northwind?driver=SQL Server Native Client 11.0')

#What is this? Check declarative_base() method
Base = declarative_base()

#it's clear
class Author(Base):
    __tablename__='AuthorsORM'
    Id = Column(Integer,primary_key=True)
    FirstName=Column(String)
    LastName=Column(String)

#here probabl we create our metadata
Base.metadata.bind=engine
Base.metadata.create_all()

#open session it's clear
Session=sessionmaker(bind=engine)
ses=Session()

#the way to add and commit data
ses.add_all(
    [Author(Id=1,FirstName='Michaił',LastName='Bułkahov')]
)

#save in database our changes
ses.commit()

#execute our query
results=ses.query(Author).all()

#printing data
for author in results:
    print(author.FirstName, author.LastName)