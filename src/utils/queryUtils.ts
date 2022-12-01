export function placeholders(value: unknown[]): string[] {
  return value.map(() => '?');
}

export function indexPlaceholders(value: unknown[]): string[] {
  return value.map((_, index) => `$${index + 1}`);
}
