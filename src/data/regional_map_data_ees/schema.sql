

CREATE SEQUENCE indicators_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 7 NO CYCLE;
CREATE SEQUENCE filters_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 69 NO CYCLE;
CREATE SEQUENCE locations_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 11 NO CYCLE;
CREATE SEQUENCE time_periods_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 2 NO CYCLE;
CREATE SEQUENCE data_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 53349 NO CYCLE;

CREATE TABLE "data"(id BIGINT, time_period VARCHAR, time_identifier VARCHAR, geographic_level VARCHAR, country_code VARCHAR, country_name VARCHAR, region_code VARCHAR, region_name VARCHAR, YAG VARCHAR, SECTIONNAME VARCHAR, qualification_TR VARCHAR, subject_name VARCHAR, trained_in_region VARCHAR, living_in_region VARCHAR, difference VARCHAR, difference_prop VARCHAR, earnings_median VARCHAR, number_of_providers VARCHAR);
CREATE TABLE time_periods(id UINTEGER PRIMARY KEY DEFAULT(nextval('time_periods_seq')), "year" VARCHAR NOT NULL, identifier VARCHAR, ordering UINTEGER NOT NULL);
CREATE TABLE locations(id UINTEGER PRIMARY KEY DEFAULT(nextval('locations_seq')), "level" VARCHAR NOT NULL, code VARCHAR DEFAULT(''), "name" VARCHAR, ordering UINTEGER);
CREATE TABLE filters(id UINTEGER PRIMARY KEY DEFAULT(nextval('filters_seq')), "label" VARCHAR NOT NULL, group_label VARCHAR NOT NULL, group_name VARCHAR NOT NULL, group_hint VARCHAR, is_aggregate BOOLEAN DEFAULT(CAST('f' AS BOOLEAN)), ordering UINTEGER);
CREATE TABLE indicators(id UINTEGER PRIMARY KEY DEFAULT(nextval('indicators_seq')), "label" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, decimal_places INTEGER, unit VARCHAR);
CREATE TABLE data_normalised(id UINTEGER PRIMARY KEY DEFAULT(nextval('data_seq')), time_period VARCHAR, time_identifier VARCHAR, geographic_level VARCHAR, "Country :: id" UINTEGER, "Country :: ordering" UINTEGER, "Region :: id" UINTEGER, "Region :: ordering" UINTEGER, "YAG :: id" UINTEGER, "YAG :: ordering" UINTEGER, "subject_name :: id" UINTEGER, "subject_name :: ordering" UINTEGER, "qualification_TR :: id" UINTEGER, "qualification_TR :: ordering" UINTEGER, "SECTIONNAME :: id" UINTEGER, "SECTIONNAME :: ordering" UINTEGER, difference VARCHAR, earnings_median VARCHAR, living_in_region VARCHAR, trained_in_region VARCHAR, number_of_providers VARCHAR, difference_prop VARCHAR);




