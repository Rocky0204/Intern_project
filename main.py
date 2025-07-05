from fastapi import FastAPI

from api.database import engine
from api.models import Base
from api.routers import all_routers

Base.metadata.create_all(bind=engine)

app = FastAPI()

for router in all_routers:
    app.include_router(router)


@app.get("/")
def hello():
    return {"message": "Hello world!"}
