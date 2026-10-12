from contextlib import asynccontextmanager
from fastapi import FastAPI
from sqlmodel import SQLModel
from app.database import engine
from app.routes import router
from app.core import init_producer,close_producer

@asynccontextmanager
async def lifespan(app:FastAPI):
	SQLModel.metadata.create_all(engine)
	init_producer() 
	yield
	close_producer()

app=FastAPI(lifespan=lifespan)
app.include_router(router)
