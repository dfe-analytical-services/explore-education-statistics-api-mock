import * as duckdb from 'duckdb';
import type { Callback, TableData } from 'duckdb';
import { noop } from 'lodash';
import formatQuery from './formatQuery';

const timerLabel = 'Ran query in';

interface DebugOptions {
  /**
   * Show placeholders in the logged query. If not true, the
   * query params are substituted into the query for convenience.
   */
  showPlaceholders?: boolean;
}

interface QueryOptions {
  debug?: boolean | DebugOptions;
}

export interface StreamResult<T> {
  [Symbol.asyncIterator](): AsyncIterator<T>;
}

export default class Database {
  private readonly db: duckdb.Database;

  constructor(destination: string = ':memory:') {
    this.db = new duckdb.Database(destination);
  }

  /**
   * Run a query without returning any results.
   */
  async run(
    query: string,
    params: (string | number)[] = [],
    options?: QueryOptions,
  ): Promise<void> {
    if (this.canDebug(options)) {
      console.time(timerLabel);
      this.logQuery(query, params, options.debug);
    }

    await new Promise((resolve, reject) => {
      this.db.run(query, ...params, ((err, result) => {
        if (this.canDebug(options)) {
          console.timeEnd(timerLabel);
        }

        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      }) as Callback<void>);
    });
  }

  /**
   * Run a query and return a list of results.
   */
  async all<TResult>(
    query: string,
    params: any[] = [],
    options?: QueryOptions,
  ): Promise<TResult[]> {
    if (this.canDebug(options)) {
      console.time(timerLabel);
      this.logQuery(query, params, options.debug);
    }

    return await new Promise((resolve, reject) => {
      this.db.all(query, ...params, ((err, result) => {
        if (this.canDebug(options)) {
          console.timeEnd(timerLabel);
        }

        if (err) {
          reject(err);
        } else {
          resolve(result as TResult[]);
        }
      }) as Callback<TableData>);
    });
  }

  /**
   * Run a query and return the first result.
   */
  async first<TResult>(
    query: string,
    params: any[] = [],
    options?: QueryOptions,
  ): Promise<TResult> {
    if (this.canDebug(options)) {
      console.time(timerLabel);
      this.logQuery(query, params, options.debug);
    }

    return await new Promise((resolve, reject) => {
      this.db.all(query, ...params, ((err, result) => {
        if (this.canDebug(options)) {
          console.timeEnd(timerLabel);
        }

        if (err) {
          reject(err);
        } else {
          resolve(result[0] as TResult);
        }
      }) as Callback<TableData>);
    });
  }

  /**
   * Stream the results of a query as a generator.
   */
  stream<TResult>(
    query: string,
    params: any[] = [],
    options?: QueryOptions,
  ): StreamResult<TResult> {
    if (this.canDebug(options)) {
      this.logQuery(query, params, options.debug);
    }

    return this.db.connect().stream(query, ...params) as StreamResult<TResult>;
  }

  close(): void {
    this.db.close(noop);
  }

  private logQuery(
    query: string,
    params: unknown[],
    options: boolean | DebugOptions,
  ) {
    console.log(
      formatQuery(
        query,
        typeof options !== 'boolean' && options.showPlaceholders ? [] : params,
      ),
    );
    console.log(params);
  }

  private canDebug(
    options?: QueryOptions,
  ): options is QueryOptions & { debug: true | DebugOptions } {
    return !!options?.debug && process.env.NODE_ENV === 'development';
  }
}
