-- Creates airflow_db alongside mmm_db
-- mmm_db is already created by the POSTGRES_DB env var in docker-compose
CREATE DATABASE airflow_db;

-- Create the raw schema inside mmm_db
-- (Postgres connects to mmm_db first by default)
\c mmm_db
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;