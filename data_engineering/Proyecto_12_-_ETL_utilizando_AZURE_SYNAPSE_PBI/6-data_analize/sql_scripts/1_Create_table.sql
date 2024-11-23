-- Crear tabla dbo.SQLPool_Table

CREATE TABLE dbo.SQLPool_Table
(
    [Education_Level] varchar(4000),
    [Line_Number] int,
    [Year] int,
    [Month] nvarchar(4000),
    [State] nvarchar(4000),
    [Labor_Force] int,
    [Employed] int,
    [Unemployed] int,
    [Industry] varchar(4000),
    [Gender] varchar(4000),
    [Date_Inserted] date,
    [UnEmployed_Rate_Percentage] real,
    [Min_Salary_USD] int,
    [Max_Salary_USD] int,
    [dense_rank] int
)
WITH
(
    DISTRIBUTION = ROUND_ROBIN,         -- Distribución de datos de forma equitativa entre nodos
    CLUSTERED COLUMNSTORE INDEX         -- Indice Columnstore para optimización en consultas analíticas
);