USE ROLE ACCOUNTADMIN;

-- Create the `TAXI_APP_ML` role
CREATE ROLE IF NOT EXISTS TAXI_APP_ML;
GRANT ROLE TAXI_APP_ML TO ROLE ACCOUNTADMIN;

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TAXI_APP_ML;



-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS TAXI_APP;
CREATE SCHEMA IF NOT EXISTS TAXI_APP.TRANSACTIONS;
CREATE SCHEMA IF NOT EXISTS TAXI_APP.TRANSACTIONS_RAW;
CREATE SCHEMA IF NOT EXISTS TAXI_APP.TRANSACTIONS_SEEDS;



-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TAXI_APP_ML; 
GRANT ALL ON DATABASE TAXI_APP to ROLE TAXI_APP_ML;
GRANT ALL ON ALL SCHEMAS IN DATABASE TAXI_APP to ROLE TAXI_APP_ML;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE TAXI_APP to ROLE TAXI_APP_ML;
GRANT ALL ON ALL TABLES IN SCHEMA TAXI_APP.TRANSACTIONS to ROLE TAXI_APP_ML;
GRANT ALL ON FUTURE TABLES IN SCHEMA TAXI_APP.TRANSACTIONS to ROLE TAXI_APP_ML;

GRANT ALL ON ALL TABLES IN SCHEMA TAXI_APP.TRANSACTIONS_RAW to ROLE TAXI_APP_ML;
GRANT ALL ON FUTURE TABLES IN SCHEMA TAXI_APP.TRANSACTIONS_RAW to ROLE TAXI_APP_ML;

GRANT ALL ON ALL TABLES IN SCHEMA TAXI_APP.TRANSACTIONS_SEEDS to ROLE TAXI_APP_ML;
GRANT ALL ON FUTURE TABLES IN SCHEMA TAXI_APP.TRANSACTIONS_SEEDS to ROLE TAXI_APP_ML;


-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt_taxi_app
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt_taxi'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='TAXI_APP_ML'
  DEFAULT_NAMESPACE='TAXI_APP_ML.TRANSACTIONS'
  COMMENT='DBT user used for data transformation';
GRANT ROLE TAXI_APP_ML to USER dbt_taxi_app;


--- CREATE FILE FORMAT


USE SCHEMA TRANSACTIONS;

create or replace file format taxi_app_file_format
  type = 'JSON'
  strip_outer_array = true;


--- CREATE THE STAGE PREDICTIONS


create or replace stage AZURE_STAGE_PREDICTIONS
URL=('azure://dataengineeringst.blob.core.windows.net/predictions')
CREDENTIALS = (AZURE_SAS_TOKEN ='?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-01-01T00:06:47Z&st=2023-11-04T16:06:47Z&spr=https&sig=24B0uoer3YMHbvbbIlkVIOJ%2Ftj7otrgoum9UjTL3Kkw%3D')
file_format=taxi_app_file_format;

--- CREATE THE STAGE TAXI-APP
create or replace stage AZURE_STAGE
URL=('azure://dataengineeringst.blob.core.windows.net/taxi-app-data')
CREDENTIALS = (AZURE_SAS_TOKEN ='?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2024-01-01T00:06:47Z&st=2023-11-04T16:06:47Z&spr=https&sig=24B0uoer3YMHbvbbIlkVIOJ%2Ftj7otrgoum9UjTL3Kkw%3D')
file_format=taxi_app_file_format;


--grant usage for the stage PREDICTIONS
GRANT USAGE ON STAGE AZURE_STAGE_PREDICTIONS TO ROLE TAXI_APP_ML;
GRANT USAGE ON FILE FORMAT TAXI_APP_FILE_FORMAT TO TAXI_APP_ML;

--grant usage for the stage TAXI-APP
GRANT USAGE ON STAGE azure_stage TO ROLE TAXI_APP_ML;
GRANT USAGE ON FILE FORMAT TAXI_APP_FILE_FORMAT TO TAXI_APP_ML;


---JSON TABLE FOR TAXI APP DATA

CREATE OR REPLACE TABLE taxi_app.transactions_raw.taxi_calls_json (
  var variant,
  DT_INGESTION_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP() 
);

---JSON TABLE FOR PREDICTIONS DATA

CREATE OR REPLACE TABLE taxi_app.transactions_raw.taxi_calls_predictions_json (
  var variant,
  DT_INGESTION_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);






