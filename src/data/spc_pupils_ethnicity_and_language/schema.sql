

CREATE SEQUENCE data_facts_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 154193 NO CYCLE;
CREATE SEQUENCE indicators_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 3 NO CYCLE;
CREATE SEQUENCE filters_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 32 NO CYCLE;
CREATE SEQUENCE locations_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 167 NO CYCLE;
CREATE SEQUENCE time_periods_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 8 NO CYCLE;

CREATE TABLE "data"(time_period VARCHAR, time_identifier VARCHAR, geographic_level VARCHAR, country_code VARCHAR, country_name VARCHAR, region_name VARCHAR, region_code VARCHAR, old_la_code VARCHAR, la_name VARCHAR, new_la_code VARCHAR, phase_type_grouping VARCHAR, ethnicity VARCHAR, "language" VARCHAR, headcount VARCHAR, percent_of_pupils VARCHAR);
CREATE TABLE time_periods(id INTEGER PRIMARY KEY DEFAULT(nextval('time_periods_seq')), "year" INTEGER NOT NULL, identifier VARCHAR);
CREATE TABLE locations(id INTEGER PRIMARY KEY DEFAULT(nextval('locations_seq')), geographic_level VARCHAR, country_code VARCHAR, country_name VARCHAR, region_name VARCHAR, region_code VARCHAR, old_la_code VARCHAR, la_name VARCHAR, new_la_code VARCHAR);
CREATE TABLE filters(id INTEGER PRIMARY KEY DEFAULT(nextval('filters_seq')), "label" VARCHAR NOT NULL, group_label VARCHAR NOT NULL, group_name VARCHAR NOT NULL, group_hint VARCHAR, is_aggregate BOOLEAN DEFAULT(CAST('f' AS BOOLEAN)));
CREATE TABLE indicators(id INTEGER PRIMARY KEY DEFAULT(nextval('indicators_seq')), "label" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, decimal_places INTEGER, unit VARCHAR);
CREATE TABLE data_facts(id BIGINT PRIMARY KEY DEFAULT(nextval('data_facts_seq')), time_period_id INTEGER NOT NULL, location_id INTEGER NOT NULL, phase_type_grouping INTEGER NOT NULL, ethnicity INTEGER NOT NULL, "language" INTEGER NOT NULL, headcount VARCHAR, percent_of_pupils VARCHAR);




