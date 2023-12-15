from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.function_definitions import functions
from app.functions import api_functions
from app.handler import OpenAIHandler
from app.models import Interaction
from app.db import Base, engine
from app.prompts import system_message
import os
from app.store import create_store
from app.db import Session, Appointment
import redis
import requests
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

r = redis.Redis(host='redis', port=6379, db=0)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
handler = OpenAIHandler(api_functions, functions, system_message)


@app.on_event("startup")
async def startup_event():
    Base.metadata.create_all(bind=engine)
    if not os.path.exists("vectorstore.pkl"):
        create_store()


@app.on_event("shutdown")
async def shutdown_event():
    os.remove("bank.db")


@app.post("/conversation")
async def query_endpoint(interaction: Interaction):
    response = handler.send_response(interaction.query)
    return {"response": response}


@app.get("/appointment")
async def get_all_appointments():
    session = Session()
    appointment = session.query(Appointment).all()
    session.close()
    return appointment


