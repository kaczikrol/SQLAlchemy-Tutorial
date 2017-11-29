from sqlalchemy import Column, Integer, String, ForeignKey, MetaData
from sqlalchemy import create_engine, Text, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, mapper,relation,sessionmaker

#CREATE ENGINE TO OUR DATABASE!
engine=create_engine('mssql+pyodbc://DOM/Northwind?driver=SQL Server Native Client 11.0')
#connection=engine.connect()
#connection.execute(text("DROP TABLE dbo.users DROP TABLE dbo.companies"))

#BASE OBJECT IN ORM - but what is this? Probably it's declarative model and we base on class on it
Base=declarative_base(bind=engine)

class Company(Base):
    #Here we define our table -it's clear
    __tablename__ = 'companies'
    id = Column(Integer,primary_key=True)
    name = Column(String)
    name_abbrev = Column(String(5))
    country = Column(String)

    #it's clear
    def __init__(self,name,country):
        name = self.name
        name_abbrev = self.name[:5]
        country = self.country

    def __repr__(self):
        return "<Company('%s','%s','%s')>" %(self.name,self.name_abbrev,self.country)



class User(Base):
    #Here we create our table!
    __tablename__ = 'users'
    id = Column(Integer,primary_key=True)
    login = Column(String)
    password = Column(String)
    city = Column(String)
    company_id = Column(Integer,ForeignKey('companies.id'))

    #here we bulid relation between two tables User and Company at this moment it's not clear for me
    company = relation(Company,backref=backref('companies',order_by=id))

    def __init__(self,login,password,city):
        login = self.login
        password = self.password
        city = self.city
        #country = self.country

    def __repr__(self):
        return "<User('%s','%s','%s')>" %(self.login,self.password,self.city)



#Here we simply creates our metadata as before meta=MetaData(engine)
meta=Base.metadata

#Now we execute - in this case we create tables, we cannot use here create_all(bind=engine) because
#variable meta is based on Base and Base=declarative_base(bind=engine)
meta.create_all()

#if we want have table object is simply calling:
users_table=User.__table__
companies_table=Company.__table__

