from fastapi import FastAPI

from .viewpoint import routers
import logging

app = FastAPI()

app.include_router(routers.router)

logger = logging.getLogger("uvicorn")

@app.get("/")
async def root():
    return {"message": "Hello Tile Server!"}
