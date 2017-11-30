from sqlalchemy import Column, Integer, String, ForeignKey, MetaData
from sqlalchemy import create_engine, Text, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, mapper,relation,sessionmaker
import random

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
        self.name = name
        self.name_abbrev = name[:5]
        self.country = country

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
        self.login = login
        self.password = password
        self.city = city
        #self.company_id = company_id

    def __repr__(self):
        return "<User('%s','%s','%s')>" %(self.login,self.password,self.city)

    @staticmethod
    def getKeys():
        print('login','passwor','city')



#Here we simply creates our metadata as before meta=MetaData(engine)
meta=Base.metadata

#Now we execute - in this case we create tables, we cannot use here create_all(bind=engine) because
#variable meta is based on Base and Base=declarative_base(bind=engine)
meta.create_all()

#if we want have table object is simply calling:
companies_table=Company.__table__
users_table=User.__table__

#we don't create session so nothing happen in SQL, but user exists in our cache
mike=User('MIKE001','MikeIsKing','London')
print(mike)

#let's create session
Session = sessionmaker(bind=engine)
session = Session()

arvato=Company('Arvato Polska','Poland')
bz_wbk=Company('BZ WBK Leasing','Poland')

susana=User('SUSI001','Susana2012','Berlin')

#we add this transaction to queque and it's pending to commit session after commit changes will be in table
session.add(susana)
przemek=User('KACZy','Kaczmarek','Poznan')
przemek.company_id=11

#commit provides that we insert changes to table and SQL will be execute
#session.commit()

session.add_all([
    User('JOHN002','Johny','Manchester'),
    User('PRZE123','Perla','Poznan'),
    User('JUL001','Cebulka','Warsaw'),
    susana,
    arvato,
    przemek,
    bz_wbk
])

session.commit()

##############################HOW TO USE SESSION.DIRTY?##############################
#print(session.dirty)

#############################to check pending elements################################
#print(session.new)

###############################ONE OF THE MOST IMPORTANT METHOD!######################
#session.commit()

all_users=session.query(User).all()
User.getKeys()
for row in all_users:
    print(row)

london_users=session.query(User).filter_by(city='London')
print('london users')
for row in london_users:
    print(row)


berlin_users=session.query(User).filter_by(city='Berlin')
print('berlin users')
for row in berlin_users:
    print(row)

first_join=session.query(User,Company).filter(User.company_id==Company.id)
for row in first_join:
    print(row)