import os
from app.db import Session, Appointment
from app.prompts import QA_PROMPT
import json
from langchain.llms import OpenAI
from langchain.chains import RetrievalQA
from app.store import get_vectorstore

def create_appointment(appointment_text: str):
    session = Session()
    appointment = Appointment(appointment=appointment_text)
    session.add(appointment)
    session.commit()
    session.close()
    return "Appointment created"

def ask_vector_db(question: str):
    llm = OpenAI(openai_api_key=os.environ.get("OPENAI_API_KEY"))
    qa = RetrievalQA.from_chain_type(
        llm=llm,
        chain_type="stuff",
        retriever=get_vectorstore().as_retriever(),
        chain_type_kwargs={"prompt": QA_PROMPT},
    )
    result = qa.run(question)
    return result

api_functions = {
    "create_appointment": create_appointment,
    "ask_vector_db": ask_vector_db,
}

### Just for initialisation
