from sqlalchemy import create_engine
from sqlalchemy import schema, types
from sqlalchemy import text

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

-------------------------------------------
IF OBJECT_ID(N'dbo.Authors',N'u') is not null 
DROP TABLE dbo.Authors
GO
CREATE TABLE Authors(AuthorId INTEGER PRIMARY KEY, Name TEXT);
INSERT INTO Authors VALUES(1, 'Jane Austen');
INSERT INTO Authors VALUES(2, 'Leo Tolstoy');
INSERT INTO Authors VALUES(3, 'Joseph Heller');
INSERT INTO Authors VALUES(4, 'Charles Dickens')

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
      {"Id":2, "Name":"Mercedes", "Price":57127})

for line in data:
    connection.execute(text("INSERT INTO Cars(Id, Name, Price) VALUES(:Id, :Name, :Price)"),**line)
