

CREATE SEQUENCE indicators_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 4 NO CYCLE;
CREATE SEQUENCE filters_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 50 NO CYCLE;
CREATE SEQUENCE locations_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 170 NO CYCLE;
CREATE SEQUENCE time_periods_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 8 NO CYCLE;
CREATE SEQUENCE data_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 347831 NO CYCLE;

CREATE TABLE "data"(id BIGINT, time_period VARCHAR, time_identifier VARCHAR, geographic_level VARCHAR, country_code VARCHAR, country_name VARCHAR, region_name VARCHAR, region_code VARCHAR, old_la_code VARCHAR, la_name VARCHAR, new_la_code VARCHAR, phase_type_grouping VARCHAR, characteristic_group VARCHAR, characteristic VARCHAR, fsm_eligibility VARCHAR, number_of_pupils VARCHAR, denominator VARCHAR, percent_of_pupils VARCHAR);
CREATE TABLE time_periods(id UINTEGER PRIMARY KEY DEFAULT(nextval('time_periods_seq')), "year" VARCHAR NOT NULL, identifier VARCHAR, ordering UINTEGER NOT NULL);
CREATE TABLE locations(id UINTEGER PRIMARY KEY DEFAULT(nextval('locations_seq')), "level" VARCHAR NOT NULL, code VARCHAR DEFAULT(''), "name" VARCHAR, ordering UINTEGER);
CREATE TABLE filters(id UINTEGER PRIMARY KEY DEFAULT(nextval('filters_seq')), "label" VARCHAR NOT NULL, group_label VARCHAR NOT NULL, group_name VARCHAR NOT NULL, group_hint VARCHAR, is_aggregate BOOLEAN DEFAULT(CAST('f' AS BOOLEAN)), ordering UINTEGER);
CREATE TABLE indicators(id UINTEGER PRIMARY KEY DEFAULT(nextval('indicators_seq')), "label" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, decimal_places INTEGER, unit VARCHAR);
CREATE TABLE data_normalised(id UINTEGER PRIMARY KEY DEFAULT(nextval('data_seq')), time_period VARCHAR, time_identifier VARCHAR, geographic_level VARCHAR, "Country :: id" UINTEGER, "Country :: ordering" UINTEGER, "Region :: id" UINTEGER, "Region :: ordering" UINTEGER, "LocalAuthority :: id" UINTEGER, "LocalAuthority :: ordering" UINTEGER, "characteristic :: id" UINTEGER, "characteristic :: ordering" UINTEGER, "fsm_eligibility :: id" UINTEGER, "fsm_eligibility :: ordering" UINTEGER, "phase_type_grouping :: id" UINTEGER, "phase_type_grouping :: ordering" UINTEGER, number_of_pupils VARCHAR, percent_of_pupils VARCHAR, denominator VARCHAR);




