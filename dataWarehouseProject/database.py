#!/usr/bin/python3
from typing import Dict
import psycopg2 as pg
from sqlalchemy import create_engine

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

# class DatabaseInterface(metaclass=Singleton):
#     def __init__(self) -> None:
#         '''Interface for romes db'''
#         self.host = 'ddcmstud.ugent.be'
#         self.user = 'student16'
#         self.password = 'rrxnkuefhm'
#         self.dbname = 'student16'
#         self.port='8081'
#         self.connection={}

class DatabaseInterface(metaclass=Singleton):
    def __init__(self) -> None:
        '''Interface for romes db'''
        self.host = 'localhost'
        self.user = 'postgres'
        self.password = 'Mamanmaa21'
        self.dbname = 'postgres'
        self.port='8081'
        self.connection={}

    def getConnection(self, engine = False):
        '''Returns connection object

        Args:
            engine (bool): when true, creates the connection object through sql alchemy.
                This flag is mandatory when using pandas

        Returns:
            Connection: connection object to the database
        '''
        if self.connection.get(engine):
            return self.connection[engine]
        if engine:
            engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}')
            self.connection[engine] = engine.connect()
            return self.connection[engine]
        else:
            connection_string = f"dbname='{self.dbname}' user='{self.user}' host='{self.host}' password='{self.password}' port='{self.port}'"
            self.connection[engine] = pg.connect(connection_string)
            return self.connection[engine]

 
    def drop_table(self,tableName):
        con = self.getConnection()
        cur = con.cursor()
        cur.execute('drop table if exists '+tableName)
        con.commit()
        cur.close()       

    def run_procedure(self,procedureName, *args):
        con = self.getConnection()
        cur = con.cursor()
        print(f'CALL {procedureName}({",".join(["%s"]*len(args))})', args)
        cur.execute(f'CALL {procedureName}({",".join(["%s"]*len(args))})', args)
        con.commit()
        cur.close()       
