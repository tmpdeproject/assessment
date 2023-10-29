#Purpose: Simple Helper functions to deal with SQL Alchemy sessions.
#Could be certainly extended into a OOP class that encapsulate SQL alchemy functionality eg. bulkinsert, add_all bulksave etc.
#Aims to decouple airflow with SQLachemy engine/sessions.

#Date: 27/10/2023
#Author: Mark Grech

from sqlalchemy.orm import sessionmaker

def create_session(engine):
    Session = sessionmaker(bind=engine)
    return Session() #return an instance

def begin_session(session):
    if session:
        session.begin()

def rollback_session(session):
    if session:
        session.rollack()

def flush_session(session):
    if session:
        session.flush()

def commit_session(session):
    if session:
        session.commit()
        session.close()