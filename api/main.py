from fastapi import FastAPI

from api.routers.all_routers import all_routers

app = FastAPI()

for router in all_routers:
    app.include_router(router)


@app.get("/")
def hello():
    return {"message": "Hello world!"}
