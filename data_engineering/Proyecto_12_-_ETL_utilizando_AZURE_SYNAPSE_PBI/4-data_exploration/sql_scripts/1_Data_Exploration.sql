-- HTTPS

SELECT *
FROM 
    OPENROWSET(
        BULK 'https://adlsproyectos.dfs.core.windows.net/raw/Unemployment.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'

    ) AS [data]

-- ABFSS

SELECT *
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0'

    ) AS [data]   

--HEADER ROW

SELECT *
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) AS [data]

-- COLUMNAS ESPECIFICAS

SELECT [Line Number],[Month]
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) AS [data]

-- ALIAS

SELECT [data].[Line Number],[data].[Month]
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) AS [data]

-- WITH

SELECT [data].[Line Number],[data].[Month]
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) 
    WITH (
        [Line Number] INT,
        [Month] VARCHAR(10)
    ) AS [data]

-- FIRSTROW

SELECT *
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        FIRSTROW = 2,
        PARSER_VERSION = '2.0'
    ) AS [data]

-- FIELDTERMINATOR

SELECT *
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        PARSER_VERSION = '2.0'
    ) AS [data]
