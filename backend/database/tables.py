from sqlalchemy import Table, Column, Integer, String, BigInteger, TIMESTAMP
from backend.database.init import metadata

messages = Table(
    "messages",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("consumer_name", String, nullable=False),  # updated from consumer_id
    Column("topic", String, nullable=False),
    Column("partition", Integer, nullable=False),
    Column("message_offset", BigInteger, nullable=False),
    Column("key", String),
    Column("value", String, nullable=False),
    Column("timestamp", TIMESTAMP),
)
