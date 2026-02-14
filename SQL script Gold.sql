--- Read Parquet data from silver layer
SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://ecommhenrystorage.blob.core.windows.net/ecommercedata/silver/',
        FORMAT = 'PARQUET'
    ) AS result1

--- Create Gold Schema and Create View Gold for final representation from silver layer
create schema gold

create view gold.final
as 
SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://ecommhenrystorage.blob.core.windows.net/ecommercedata/silver/',
        FORMAT = 'PARQUET'
    ) AS result1

--- Read data from view gold.final
select * from gold.final

--- Create masterkey for database Encryption and Create Credential to access the datalake
CREATE MASTER KEY ENCRYPTION BY PASSWORD = '5:XM?2x@h[3`';
CREATE DATABASE SCOPED CREDENTIAL henryecommerce WITH IDENTITY = 'Managed Identity';

--- View all credential
select * from sys.database_credentials

--- Create CETAS for data serving layer
CREATE EXTERNAL FILE FORMAT extfileformat WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL DATA SOURCE goldlayer WITH (
    LOCATION = 'https://ecommhenrystorage.blob.core.windows.net/ecommercedata/gold/',
    CREDENTIAL = henryecommerce
);

CREATE EXTERNAL TABLE gold.finaltable WITH (
        LOCATION = 'DataServing',
        DATA_SOURCE = goldlayer,
        FILE_FORMAT = extfileformat
) AS
SELECT * FROM gold.final;

--- Read Data from External table
select * from gold.finaltable