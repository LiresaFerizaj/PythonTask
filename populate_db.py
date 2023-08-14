import json
import uuid
from sqlalchemy import create_engine, Column, Integer, String, Text, JSON, DateTime,VARCHAR, text,ForeignKey
from sqlalchemy.orm import sessionmaker,relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import pandas as pd
import re

DATABASE_URL = "mssql+pyodbc://liresa:124324@LIRESAQX\\SQLEXPRESS/arxivdata?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


class Paper(Base):
    __tablename__ = "papers"

    id = Column(VARCHAR(255), primary_key=True, unique=True)
    title = Column(String(700), index=True)
    abstract = Column(Text)
    authors = Column(JSON)
    categories = Column(JSON)
    publication_date = Column(DateTime, default=datetime.utcnow)


Base.metadata.create_all(bind=engine)

json_file_path = "arxiv-metadata-oai-snapshot.json"

def get_metadata():
    with open(json_file_path, 'r') as f:
        for line in f:
            yield line


metadata = get_metadata()
record_dicts = []


for i, paper in enumerate(metadata):
    paper = json.loads(paper)
    try:
        paper_id = paper['id']

        # Clean data
        authors_string = paper['authors'].strip('"') 
        authors_list = [author.strip() for author in authors_string.split(" and ")]

        categories_string = paper['categories'].strip('"')  
        categories_list = [category.strip() for category in categories_string.split(" ")]

        publication_date_str = paper['update_date']
        try:
            publication_date = datetime.fromtimestamp(int(publication_date_str) / 1000.0)
        except ValueError:
            publication_date = datetime.strptime(publication_date_str, '%Y-%m-%d')

        abstract_cleaned = re.sub("<.*?>", "", paper['abstract'])  
        abstract_cleaned = abstract_cleaned.replace("\n", " ") 

        title_cleaned = re.sub("[^a-zA-Z0-9\s]", "", paper['title'])  
        title_cleaned = title_cleaned.replace("\n", " ")  
        record = {
            'id': paper_id,
            'title': title_cleaned,
            'abstract': abstract_cleaned,
            'authors': authors_list,
            'categories': categories_list,
            'publication_date': publication_date
        }
        record_dicts.append(record)
    except Exception as e:
        print(f"Failed to process paper {i}: {e}")

def populate_db():
    try:
        db = SessionLocal()

        for record in record_dicts:
            paper_record = Paper(
                id=record['id'],
                title=record['title'],
                abstract=record['abstract'],
                authors=record['authors'],
                categories=record['categories'],
                publication_date=record['publication_date']
            )
            
            db.add(paper_record)
            db.commit()

        print("Data insertion successful.")
    except Exception as e:
        db.rollback()
        print(f"Data insertion failed: {e}")
    finally:
        db.close()

populate_db()
