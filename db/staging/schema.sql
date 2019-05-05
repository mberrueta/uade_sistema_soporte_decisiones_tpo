DROP DATABASE IF EXISTS dicision_support_staging;
CREATE DATABASE dicision_support_staging;

\connect dicision_support_staging


----------------------------------------------------------------------
-- Tables
----------------------------------------------------------------------

DROP TABLE IF EXISTS fact_xx;
DROP TABLE IF EXISTS dim_xx;

CREATE TABLE dim_xx
(
  id                       CHARACTER VARYING(36) NOT NULL,
  name                     CHARACTER VARYING(200),
  a                        BOOLEAN
);
ALTER TABLE dim_xx ADD CONSTRAINT dim_xx_id_pk PRIMARY KEY (id);

CREATE TABLE dim_dates
(
  id                       INT NOT NULL,
  date_actual              DATE NOT NULL,
  epoch                    BIGINT NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_week              INT NOT NULL,
  day_of_month             INT NOT NULL,
  day_of_quarter           INT NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             BOOLEAN NOT NULL
);

ALTER TABLE dim_dates ADD CONSTRAINT dim_dates_id_pk PRIMARY KEY (id);


DROP INDEX IF EXISTS dim_dates_date_actual_idx;

CREATE INDEX dim_dates_date_actual_idx ON dim_dates(date_actual);