import { GeographicLevel } from '../schema';

export function locationIdColumn(geographicLevel: GeographicLevel) {
  return `${geographicLevel} :: id`;
}

export function locationOrderColumn(geographicLevel: GeographicLevel) {
  return `${geographicLevel} :: ordering`;
}

export function filterIdColumn(filter: string) {
  return `${filter} :: id`;
}

export function filterOrderColumn(filter: string) {
  return `${filter} :: ordering`;
}
