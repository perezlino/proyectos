-- Copiar datos en la tabla dbo.SQLPool_Table

COPY INTO dbo.SQLPool_Table
(
    Education_Level,  -- Columna 1
    Line_Number,      -- Columna 2
    Year,             -- Columna 3
    Month,            -- Columna 4
    State,            -- Columna 5
    Labor_Force,      -- Columna 6
    Employed,         -- Columna 7
    Unemployed,       -- Columna 8
    Industry,         -- Columna 9
    Gender,           -- Columna 10
    Date_Inserted,    -- Columna 11
    UnEmployed_Rate_Percentage, -- Columna 12
    Min_Salary_USD,   -- Columna 13
    Max_Salary_USD,   -- Columna 14
    dense_rank        -- Columna 15
)
FROM 'https://adlsproyectos.dfs.core.windows.net/processed/processed/part-00000-82652987-a413-4adc-bb71-ac7beec05c46-c000.snappy.parquet'
WITH
(
    FILE_TYPE = 'PARQUET'  -- Tipo de archivo (en este caso, Parquet)
);


-- Verificar que los datos se hayan cargado en la tabla

SELECT TOP 10 * FROM dbo.SQLPool_Table