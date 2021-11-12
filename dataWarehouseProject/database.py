#!/usr/bin/python3
from typing import Dict
import psycopg2 as pg
from sqlalchemy import create_engine

class DatabaseInterface:
    def __init__(self) -> None:
        '''Interface for romes db'''
        self.host = 'ddcmstud.ugent.be'
        self.user = 'student16'
        self.password = 'rrxnkuefhm'
        self.dbname = 'student16'
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
        cur.execute('drop table '+tableName)
        con.commit()
        cur.close()
        con.close()

