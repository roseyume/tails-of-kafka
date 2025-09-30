from databases import Database
from sqlalchemy import MetaData

DATABASE_URL = "postgresql://postgres:password@localhost:5432/kafkadashboard"


# Async database connection
database = Database(DATABASE_URL)
metadata = MetaData()
