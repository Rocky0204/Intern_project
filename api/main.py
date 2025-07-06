from fastapi import FastAPI

from api.database import engine
from api.models import Base
from api.routers.all_routers import all_routers

# IMPORTANT: Base.metadata.create_all(bind=engine) should typically NOT be called
# directly in main.py for production, as it's meant for migrations.
# For testing, it's handled by conftest.py.
# Base.metadata.create_all(bind=engine) # Keep this commented out or removed if present

app = FastAPI()

# Include all routers from the all_routers list
for router in all_routers:
    app.include_router(router)


@app.get("/")
def hello():
    return {"message": "Hello world!"}
