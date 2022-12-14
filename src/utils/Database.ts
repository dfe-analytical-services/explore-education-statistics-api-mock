import * as duckdb from 'duckdb';

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
      this.db.run(query, ...params, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Run a {@param query} using some {@param params}.
   * Returns a list of results.
   */
  async all<TResult>(query: string, params: any[] = []): Promise<TResult[]> {
    return await new Promise((resolve, reject) => {
      this.db.all(query, ...params, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  }

  /**
   * Get the first result from a {@param query} using some {@param params}.
   */
  async first<TResult>(query: string, params: any[] = []): Promise<TResult> {
    return await new Promise((resolve, reject) => {
      this.db.all(query, ...params, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result[0]);
        }
      });
    });
  }

  /**
   * Stream the results of a {@param query} using some{@param params}.
   * Returns a generator.
   */
  stream<TResult>(
    query: string,
    params: any[] = []
  ): Generator<TResult, void, []> {
    return this.db.stream(query, ...params);
  }

  close(): void {
    this.db.close();
  }
}
