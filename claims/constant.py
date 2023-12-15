from dataclasses import dataclass
from datetime import datetime
import os, sys
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
DATABASE_NAME = "claims-db"
from dotenv import load_dotenv
load_dotenv()
@dataclass
class EnvironmentVariable:
    mongo_db_url = os.getenv("MONGO_DB_URL")
    database_name = os.getenv("DATABASE_NAME")


env_var = EnvironmentVariable()
