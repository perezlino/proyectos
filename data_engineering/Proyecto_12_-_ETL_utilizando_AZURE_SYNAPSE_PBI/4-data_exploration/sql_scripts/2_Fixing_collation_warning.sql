--HEADER ROW

SELECT *
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) AS [data]


-- CREATE DATABASE

CREATE DATABASE ProjectDB


-- USAR EXPLICITAMENTE COLLATION SOBRE LAS COLUMNAS

SELECT [data].[Line Number],[data].[Month]
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) 
    WITH (
        [Line Number] VARCHAR(10) COLLATE Latin1_General_100_CI_AS_KS_SC_UTF8,
        [Month] VARCHAR(10) COLLATE Latin1_General_100_CI_AS_KS_SC_UTF8
    ) AS [data]


-- CAMBIAR COLLATION PARA TODA LA BASE DE DATOS

ALTER DATABASE ProjectDB
COLLATE Latin1_General_100_CI_AS_KS_SC_UTF8


-- VOLVER A EJECUTAR 

SELECT [data].[Line Number],[data].[Month]
FROM 
    OPENROWSET(
        BULK 'abfss://raw@adlsproyectos.dfs.core.windows.net/Unemployment.csv',
        FORMAT = 'CSV',
        HEADER_ROW = TRUE,
        PARSER_VERSION = '2.0'
    ) 
    WITH (
        [Line Number] VARCHAR(10),
        [Month] VARCHAR(10)
    ) AS [data]

