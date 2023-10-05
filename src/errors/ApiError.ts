import { ApiErrorViewModel, ErrorDictionary } from '../schema';

interface ApiErrorOptions extends ApiErrorViewModel {
  contentType?: string;
}

export default class ApiError extends Error implements ApiErrorViewModel {
  public title: string;
  public status: number;
  public type: string;
  public errors?: ErrorDictionary;

  constructor({ title, type, status, errors }: ApiErrorOptions) {
    super(title);
    this.title = title;
    this.type = type;
    this.status = status;
    this.errors = errors;
  }
}
