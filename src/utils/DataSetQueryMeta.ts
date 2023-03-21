import { GeographicLevel } from '../schema';
import { Indicator } from '../types/dbSchemas';

export interface DataSetQueryMeta {
  geographicLevels: Set<GeographicLevel>;
  locationCols: string[];
  filterCols: string[];
  indicators: Indicator[];
}
