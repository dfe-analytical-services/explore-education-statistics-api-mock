import { GeographicLevel } from '../schema';
import { IndicatorRow } from '../types/dbSchemas';

export interface DataSetQueryMeta {
  geographicLevels: Set<GeographicLevel>;
  locationCols: string[];
  filterCols: string[];
  indicators: IndicatorRow[];
}
