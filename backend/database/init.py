from databases import Database
from sqlalchemy import MetaData

DATABASE_URL = "postgresql://postgres:password@pg-local:5432/kafkadashboard"


# Async database connection
database = Database(DATABASE_URL)
metadata = MetaData()
