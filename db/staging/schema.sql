SET CLIENT_ENCODING TO Unicode
DROP DATABASE IF EXISTS support_system_decisions_staging;
CREATE DATABASE support_system_decisions_staging;

\connect support_system_decisions_staging


----------------------------------------------------------------------
-- Tables
----------------------------------------------------------------------

DROP TABLE IF EXISTS fact_orders;
DROP TABLE IF EXISTS fact_order_details;
DROP TABLE IF EXISTS fact_deliveries;
DROP TABLE IF EXISTS fact_shippings;
DROP TABLE IF EXISTS dim_categories;
DROP TABLE IF EXISTS dim_addresses;
DROP TABLE IF EXISTS dim_clients;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_providers;

CREATE TABLE fact_orders
(
  id                       INT,
  id_date                  INT,
  id_client                CHAR(15) NOT NULL
);
ALTER TABLE fact_orders ADD CONSTRAINT fact_orders_id_pk PRIMARY KEY (id);

CREATE TABLE fact_deliveries
(
  id                       INT,
  id_date                  INT,
  id_client                CHAR(15) NOT NULL
);
ALTER TABLE fact_deliveries ADD CONSTRAINT fact_deliveries_id_pk PRIMARY KEY (id);

CREATE TABLE fact_shippings
(
  id                       INT,
  id_date                  INT,
  id_client                CHAR(15) NOT NULL
);
ALTER TABLE fact_shippings ADD CONSTRAINT fact_shippings_id_pk PRIMARY KEY (id);

CREATE TABLE fact_order_details
(
  id                       SERIAL PRIMARY KEY,
  id_order                 INT,
  id_product               INT,
  quantity                 INT,
  currency                 CHAR(3) NOT NULL,
  unit_price               NUMERIC (8, 2),
  discount                 NUMERIC (4, 2),
  total_price              NUMERIC (8, 2)
);
ALTER TABLE fact_order_details ADD CONSTRAINT fact_order_details_id_pk PRIMARY KEY (id);

CREATE TABLE dim_categories
(
  id                       INT,
  name                     CHARACTER VARYING(200)
);
ALTER TABLE dim_categories ADD CONSTRAINT dim_categories_id_pk PRIMARY KEY (id);

CREATE TABLE dim_clients
(
  id                       CHAR(15) NOT NULL,
  company_name             CHARACTER VARYING(200),
  name                     CHARACTER VARYING(200),
  lastname                 CHARACTER VARYING(200),
  id_address               CHAR(15) NOT NULL
);
ALTER TABLE dim_clients ADD CONSTRAINT dim_clients_id_pk PRIMARY KEY (id);

CREATE TABLE dim_addresses
(
  id                       CHAR(15) NOT NULL,
  state                    CHARACTER VARYING(200),
  region                   CHARACTER VARYING(200),
  country                  CHARACTER VARYING(200),
  postal_code              CHARACTER VARYING(200)
);
ALTER TABLE dim_addresses ADD CONSTRAINT dim_addresses_id_pk PRIMARY KEY (id);

CREATE TABLE dim_products
(
  id                       INT NOT NULL,
  name                     CHARACTER VARYING(200),
  id_category              INT,
  id_provider              INT,
  suspended                BOOLEAN
);
ALTER TABLE dim_products ADD CONSTRAINT dim_products_id_pk PRIMARY KEY (id);

CREATE TABLE dim_providers
(
  id                       INT NOT NULL,
  name                     CHARACTER VARYING(200),
  id_address               CHAR(15) NOT NULL
);
ALTER TABLE dim_providers ADD CONSTRAINT dim_providers_id_pk PRIMARY KEY (id);

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