from sqlalchemy import Column, Integer, String, Float, ForeignKey, create_engine
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Appointment(Base):
    __tablename__ = "appointments"
    id = Column(Integer, primary_key=True)
    appointment = Column(String)

    def to_json(self):
        return {"id": self.id, "review": self.appointment}

# Setting up the engine and session
engine = create_engine("sqlite:///bank.db")
Session = sessionmaker(bind=engine)
