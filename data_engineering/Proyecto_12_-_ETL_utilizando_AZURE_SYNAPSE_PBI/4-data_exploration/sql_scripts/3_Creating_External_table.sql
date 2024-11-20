-- Crear PARQUET FORMAT

CREATE EXTERNAL FILE FORMAT parquet_format
WITH(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


-- EXTERNAL DATA SOURCE 

CREATE EXTERNAL DATA SOURCE refined_path
WITH(
    LOCATION = 'abfss://refined@adlsproyectos.dfs.core.windows.net/'
)


-- Crear EXTERNAL TABLE

CREATE EXTERNAL TABLE Ext_Table
WITH(
    LOCATION = 'ExtNew',
    DATA_SOURCE = refined_path,
    FILE_FORMAT = parquet_format

) AS SELECT  [Line Number],[Year],[Month], [State],[Labor Force],[Employed],[Unemployed],[Unemployment Rate],[Industry],[Gender],[Education Level],
    [Date Inserted],[Aggregation Level],[Data Accuracy]
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) AS [data]


SELECT * FROM Ext_Table