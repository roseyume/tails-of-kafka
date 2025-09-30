-- -- Create the database if not using POSTGRES_DB (optional)
-- CREATE DATABASE kafkadashboard;

-- -- Connect to the database if needed
-- \c kafkadashboard;

-- Create messages table
CREATE TABLE IF NOT EXISTS messages (
    id SERIAL PRIMARY KEY,
    consumer_name VARCHAR(255) NOT NULL,
    topic VARCHAR(255) NOT NULL,
    partition INT NOT NULL,
    message_offset BIGINT NOT NULL,
    key TEXT,
    value TEXT,
    timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL
);

-- Optional: add index for faster queries
CREATE INDEX IF NOT EXISTS idx_messages_consumer_topic ON messages(consumer_name, topic);
