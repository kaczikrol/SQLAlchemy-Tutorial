from sqlalchemy import Table,Column,String,Integer, MetaData, select
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('mssql+pyodbc://DOM/Northwind?driver=SQL Server Native Client 11.0')

Base = declarative_base(bind=engine)

class Category(Base):
    __tablename__='Categories'
    category_id = Column(Integer,primary_key=True)
    category_name = Column(String)
    description = Column(String)

    def __init__(self,name,description):
        self.name = name
        self.description = description

    def __repr__(self):
        return "<Category('%s','%s')>" %(self.name,self.description)

    def getName(self):
        return self.name

    def getDescription(self):
        return self.description

meta = Base.metadata
category_table=[]
connection = engine.connect()

with connection as conn:
    metadata=MetaData(engine)
    categories=Table('Categories',metadata,autoload=True)
    stm=select([categories.c.CategoryName, categories.c.Description])
    rst=conn.execute(stm)
    print(rst.keys())
    for row in rst:
        category_table.append(Category(row[0],row[1]))
        #print(row)

conn.close()

for row in category_table:
    print('Name: ',row.getName(),', Desription: ',row.getDescription())