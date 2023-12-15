
import pymongo
import os
from claims.constant import env_var
import ssl

mongo_client = pymongo.MongoClient(env_var.mongo_db_url)