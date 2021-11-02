#!/usr/bin/python3
from typing import Dict
import psycopg2 as pg
from sqlalchemy import create_engine

class DatabaseInterface:
    def __init__(self) -> None:
        '''Interface for romes db'''
        self.host = '127.0.0.1'
        self.user = 'postgres'
        self.password = 'Mamanmaa21'
        self.dbname = 'postgres'
        self.port='8081'

    def getConnection(self, engine = False):
        '''Returns connection object

        Args:
            engine (bool): when true, creates the connection object through sql alchemy.
                This flag is mandatory when using pandas

        Returns:
            Connection: connection object to the database
        '''
        if engine:
            engine = create_engine(f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}')
            return engine.connect()
        else:
            connection_string = f"dbname='{self.dbname}' user='{self.user}' host='{self.host}' password='{self.password}' port='{self.port}'"
            return pg.connect(connection_string)

 
    def drop_table(self,tableName):
        con = self.getConnection()
        cur = con.cursor()
        cur.execute('drop table if exists '+tableName)
        cur.close()
        con.close()

