

CREATE SEQUENCE indicators_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 53 NO CYCLE;
CREATE SEQUENCE filters_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 5 NO CYCLE;
CREATE SEQUENCE locations_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 163 NO CYCLE;
CREATE SEQUENCE time_periods_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 14 NO CYCLE;
CREATE SEQUENCE data_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 5886 NO CYCLE;

CREATE TABLE indicators(id UINTEGER PRIMARY KEY DEFAULT(nextval('indicators_seq')), "label" VARCHAR NOT NULL, "name" VARCHAR NOT NULL, decimal_places INTEGER, unit VARCHAR);
CREATE TABLE filters(id UINTEGER PRIMARY KEY DEFAULT(nextval('filters_seq')), "label" VARCHAR NOT NULL, group_label VARCHAR NOT NULL, group_name VARCHAR NOT NULL, group_hint VARCHAR, is_aggregate BOOLEAN DEFAULT(CAST('f' AS BOOLEAN)));
CREATE TABLE locations(id UINTEGER PRIMARY KEY DEFAULT(nextval('locations_seq')), "level" VARCHAR NOT NULL, code VARCHAR DEFAULT(''), "name" VARCHAR);
CREATE TABLE time_periods(id UINTEGER PRIMARY KEY DEFAULT(nextval('time_periods_seq')), "year" UINTEGER NOT NULL, identifier VARCHAR, ordering UINTEGER NOT NULL);
CREATE TABLE "data"(id BIGINT PRIMARY KEY, time_period VARCHAR, time_identifier VARCHAR, geographic_level VARCHAR, country_code VARCHAR, country_name VARCHAR, region_code VARCHAR, region_name VARCHAR, new_la_code VARCHAR, la_name VARCHAR, old_la_code VARCHAR, school_type VARCHAR, num_schools VARCHAR, enrolments VARCHAR, present_sessions VARCHAR, overall_attendance VARCHAR, approved_educational_activity VARCHAR, overall_absence VARCHAR, authorised_absence VARCHAR, unauthorised_absence VARCHAR, late_sessions VARCHAR, possible_sessions VARCHAR, reason_present_am VARCHAR, reason_present_pm VARCHAR, reason_present VARCHAR, reason_l_present_late_before_registers_closed VARCHAR, reason_i_authorised_illness VARCHAR, reason_m_authorised_medical_dental VARCHAR, reason_r_authorised_religious_observance VARCHAR, reason_s_authorised_study_leave VARCHAR, reason_t_authorised_grt_absence VARCHAR, reason_h_authorised_holiday VARCHAR, reason_e_authorised_excluded VARCHAR, reason_c_authorised_other VARCHAR, reason_b_aea_education_off_site VARCHAR, reason_d_aea_dual_registration VARCHAR, reason_j_aea_interview VARCHAR, reason_p_aea_approved_sporting_activity VARCHAR, reason_v_aea_educational_visit_trip VARCHAR, reason_w_aea_work_experience VARCHAR, reason_g_unauthorised_holiday VARCHAR, reason_u_unauthorised_late_after_registers_closed VARCHAR, reason_o_other_unauthorised VARCHAR, reason_n_no_reason_yet VARCHAR, reason_x_not_attending_covid_non_compulsory VARCHAR, total_num_schools VARCHAR, total_enrolments VARCHAR, attendance_perc VARCHAR, overall_absence_perc VARCHAR, authorised_absence_perc VARCHAR, unauthorised_absence_perc VARCHAR, illness_perc VARCHAR, appointments_perc VARCHAR, unauth_hol_perc VARCHAR, unauth_oth_perc VARCHAR, unauth_late_registers_closed_perc VARCHAR, unauth_not_yet_perc VARCHAR, auth_religious_perc VARCHAR, auth_study_perc VARCHAR, auth_grt_perc VARCHAR, auth_holiday_perc VARCHAR, auth_excluded_perc VARCHAR, auth_other_perc VARCHAR, covid_non_compulsory_perc VARCHAR);




