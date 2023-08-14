import json
from fastapi import FastAPI
from populate_db import engine
import pandas as pd
from fastapi import Response


app = FastAPI()

@app.get("/papers")
async def get_all_papers(skip: int = 0):
    result= pd.read_sql("SELECT * FROM papers WITH (NOLOCK)",engine)
    result['abstract'] = result['abstract'].str.replace("\n", " ")
    result['title'] = result['title'].str.replace("\n", " ")

    records = result.to_dict(orient="records")

    def custom_json_encoder(obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return obj

    json_response = json.dumps(records, default=custom_json_encoder)
    return Response(content=json_response, media_type="application/json ;charset=utf-8")


@app.get("/papers/{paper_id}")
async def get_paper_by_id(paper_id: str):
    query = f"SELECT * FROM papers WHERE id = {paper_id}"
    result = pd.read_sql(query, engine)

    result['abstract'] = result['abstract'].str.replace("\n", " ")
    result['title'] = result['title'].str.replace("\n", " ")

    records = result.to_dict(orient="records")

    def custom_json_encoder(obj):
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return obj

    json_response = json.dumps(records, default=custom_json_encoder)
    return Response(content=json_response, media_type="application/json ;charset=utf-8")


@app.get("/authors")
async def get_all_authors():
    query = "SELECT DISTINCT authors FROM papers"  
    result = pd.read_sql(query, engine)

    authors_list = []
    for authors_string in result['authors']:
        authors = authors_string.split(" and ")  
        authors_list.extend(authors)

    authors_set = set(authors_list)  

    authors_with_ids = [{"id": idx, "name": author.strip('[]\"')} for idx, author in enumerate(authors_set, start=1)]

    return Response(content=json.dumps(authors_with_ids), media_type="application/json;charset=utf-8")


@app.get("/categories")
async def get_all_categories():
    query = "SELECT DISTINCT categories FROM papers"  
    result = pd.read_sql(query, engine)

    categories_list = []
    for categories_string in result['categories']:
        categories = categories_string.split(" ")
        categories_list.extend(categories)

    categories_set = set(categories_list)

    categories_with_ids = [{"id": idx, "name": category.strip('[]\"')} for idx, category in enumerate(categories_set, start=1)]

    return Response(content=json.dumps(categories_with_ids), media_type="application/json;charset=utf-8")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
