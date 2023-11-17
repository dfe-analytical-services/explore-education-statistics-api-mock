

CREATE SEQUENCE indicators_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 21 NO CYCLE;
CREATE SEQUENCE filters_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 14 NO CYCLE;
CREATE SEQUENCE locations_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 4453 NO CYCLE;
CREATE SEQUENCE time_periods_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 2 NO CYCLE;
CREATE SEQUENCE data_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 169431 NO CYCLE;

CREATE TABLE "data"(id BIGINT, time_period VARCHAR, time_identifier VARCHAR, geographic_level VARCHAR, country_code VARCHAR, country_name VARCHAR, region_code VARCHAR, region_name VARCHAR, old_la_code VARCHAR, new_la_code VARCHAR, la_name VARCHAR, destination_year VARCHAR, school_urn VARCHAR, school_laestab VARCHAR, school_name VARCHAR, institution_group VARCHAR, characteristic VARCHAR, data_type VARCHAR, "version" VARCHAR, cohort VARCHAR, overall VARCHAR, education VARCHAR, he VARCHAR, hel4 VARCHAR, hel5 VARCHAR, hel6 VARCHAR, fe VARCHAR, fel10no VARCHAR, fel2 VARCHAR, fel3 VARCHAR, sfc_and_ssf VARCHAR, other_edu VARCHAR, appren VARCHAR, appl4 VARCHAR, appl3 VARCHAR, appl2 VARCHAR, all_work VARCHAR, all_notsust VARCHAR, all_unknown VARCHAR);
CREATE TABLE time_periods(id UINTEGER PRIMARY KEY DEFAULT(nextval('time_periods_seq')), "year" VARCHAR NOT NULL, identifier VARCHAR, ordering UINTEGER NOT NULL);
CREATE TABLE locations(id UINTEGER PRIMARY KEY DEFAULT(nextval('locations_seq')), "level" VARCHAR NOT NULL, code VARCHAR DEFAULT(''), "name" VARCHAR, ordering UINTEGER);
CREATE TABLE filters(id UINTEGER PRIMARY KEY DEFAULT(nextval('filters_seq')), "label" VARCHAR NOT NULL, group_label VARCHAR NOT NULL, group_name VARCHAR NOT NULL, group_hint VARCHAR, is_aggregate BOOLEAN DEFAULT(CAST('f' AS BOOLEAN)), ordering UINTEGER);
CREATE TABLE indicators(id UINTEGER PRIMARY KEY DEFAULT(nextval('indicators_seq')), "label" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, decimal_places INTEGER, unit VARCHAR);
CREATE TABLE data_normalised(id UINTEGER PRIMARY KEY DEFAULT(nextval('data_seq')), time_period VARCHAR, time_identifier VARCHAR, geographic_level VARCHAR, "Country :: id" UINTEGER, "Country :: ordering" UINTEGER, "Region :: id" UINTEGER, "Region :: ordering" UINTEGER, "LocalAuthority :: id" UINTEGER, "LocalAuthority :: ordering" UINTEGER, "School :: id" UINTEGER, "School :: ordering" UINTEGER, "data_type :: id" UINTEGER, "data_type :: ordering" UINTEGER, "characteristic :: id" UINTEGER, "characteristic :: ordering" UINTEGER, "destination_year :: id" UINTEGER, "destination_year :: ordering" UINTEGER, all_unknown VARCHAR, appl3 VARCHAR, fel10no VARCHAR, fe VARCHAR, appl4 VARCHAR, he VARCHAR, appl2 VARCHAR, fel2 VARCHAR, fel3 VARCHAR, hel4 VARCHAR, hel5 VARCHAR, hel6 VARCHAR, all_notsust VARCHAR, cohort VARCHAR, other_edu VARCHAR, sfc_and_ssf VARCHAR, appren VARCHAR, education VARCHAR, overall VARCHAR, all_work VARCHAR);




