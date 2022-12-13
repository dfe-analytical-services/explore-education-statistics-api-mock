import { Response } from 'express';
import { ApiErrorViewModel, ErrorDictionary } from '../schema';

export default class ApiError extends Error implements ApiErrorViewModel {
  public title: string;
  public status: number;
  public type: string;
  public errors?: ErrorDictionary;

  constructor({ title, type, status, errors }: ApiErrorViewModel) {
    super(title);
    this.title = title;
    this.type = type;
    this.status = status;
    this.errors = errors;
  }

  public toResponse(res: Response): Response {
    return res.status(this.status).json(this);
  }
}
