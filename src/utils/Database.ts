import * as duckdb from 'duckdb';
import type { Callback, TableData } from 'duckdb';
import { noop } from 'lodash';

export interface StreamResult<T> {
  [Symbol.asyncIterator](): AsyncIterator<T>;
}

export default class Database {
  private readonly db: duckdb.Database;

  constructor(destination: string = ':memory:') {
    this.db = new duckdb.Database(destination);
  }

  /**
   * Run a {@param query} using some {@param params}.
   * Does not return any results.
   */
  async run(query: string, params: (string | number)[] = []): Promise<void> {
    await new Promise((resolve, reject) => {
      this.db.run(query, ...params, ((err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      }) as Callback<void>);
    });
  }

  /**
   * Run a {@param query} using some {@param params}.
   * Returns a list of results.
   */
  async all<TResult>(query: string, params: any[] = []): Promise<TResult[]> {
    return await new Promise((resolve, reject) => {
      this.db.all(query, ...params, ((err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result as TResult[]);
        }
      }) as Callback<TableData>);
    });
  }

  /**
   * Get the first result from a {@param query} using some {@param params}.
   */
  async first<TResult>(query: string, params: any[] = []): Promise<TResult> {
    return await new Promise((resolve, reject) => {
      this.db.all(query, ...params, ((err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result[0] as TResult);
        }
      }) as Callback<TableData>);
    });
  }

  /**
   * Stream the results of a {@param query} using some{@param params}.
   * Returns a generator.
   */
  stream<TResult>(query: string, params: any[] = []): StreamResult<TResult> {
    return this.db.connect().stream(query, ...params) as StreamResult<TResult>;
  }

  close(): void {
    this.db.close(noop);
  }
}
