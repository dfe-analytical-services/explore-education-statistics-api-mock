import { GeographicLevel } from '../schema';

export interface DataRow {
  time_period: number;
  time_identifier: string;
  geographic_level: string;
  [column: string]: string | number;
}

export interface TimePeriodRow {
  year: string;
  identifier: string;
}

export interface LocationRow {
  id: number;
  level: GeographicLevel;
  code: string;
  name: string;
}

export interface FilterRow {
  id: number;
  label: string;
  group_label: string;
  group_name: string;
  group_hint: string | null;
  is_aggregate: boolean | null;
}

export interface IndicatorRow {
  id: number;
  label: string;
  name: string;
  decimal_places: number | null;
  unit: string | null;
}
