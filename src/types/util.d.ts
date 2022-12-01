interface Dictionary<T> {
  [key: string]: T;
}

type ValuesOf<T> = {
  [K in keyof T]: T[K];
}[keyof T];
//
type UnionToIntersection<U> = (U extends any ? (k: U) => void : never) extends (
  k: infer I
) => void
  ? I
  : never;
