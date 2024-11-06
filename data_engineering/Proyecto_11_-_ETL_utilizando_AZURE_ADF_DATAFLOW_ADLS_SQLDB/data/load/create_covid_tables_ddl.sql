CREATE SCHEMA covid_reporting
GO

CREATE TABLE covid_reporting.cases_and_deaths
(
    country                 VARCHAR(100),
    country_code_2_digit    VARCHAR(2),
    country_code_3_digit    VARCHAR(3),
    population              BIGINT,
    cases_count             BIGINT,
    deaths_count            BIGINT,
    reported_date           DATE,
    source                  VARCHAR(500)
)
GO

CREATE TABLE covid_reporting.hospital_admissions_weekly
(
    country                     VARCHAR(100),
    country_code_2_digit        VARCHAR(2),
    country_code_3_digit        VARCHAR(3),
    population                  ,
    reported_year_week          VARCHAR(20),
    source                      VARCHAR(500),
    reported_week_start_date
    hospital_occupancy_count BIGINT,
    icu_occupancy_count      BIGINT,

)
GO

CREATE TABLE covid_reporting.hospital_admissions_daily
(
    country                 VARCHAR(100),
    country_code_2_digit    VARCHAR(2),
    country_code_3_digit    VARCHAR(3),
    population              VARCHAR(20),
    reported_date           DATE,
    hospital_occupancy_count BIGINT,
    icu_occupancy_count      BIGINT,
    source                  VARCHAR(500)
)
GO

