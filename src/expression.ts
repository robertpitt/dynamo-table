/**
 * Type helper to extract all valid attribute paths from an entity type.
 * Supports nested paths like "address.city" and array paths like "items[0].name".
 * 
 * This generates a union of all possible paths:
 * - Top-level keys: "id", "name", etc.
 * - Nested object paths: "address.city", "address.street", etc.
 * - Array element paths: "items[0]", "permissions[1]", etc.
 * - Nested paths within arrays: "items[0].name", etc.
 * - Array object property paths: "metadata.key" (for arrays of objects)
 * 
 * Handles optional/nullable fields and excludes functions.
 * When T is unknown, defaults to string for backward compatibility.
 * 
 * Note: Paths like "metadata.key" for arrays of objects may require special
 * handling in the implementation, as DynamoDB's contains() function works
 * differently for arrays vs nested object properties.
 */
export type AttributePath<T> = [T] extends [never]
  ? never
  : T extends object
  ? {
      [K in keyof T & string]: T[K] extends Function
        ? never
        : T[K] extends readonly unknown[] | unknown[]
        ? K | `${K}[${number}]` | (NonNullable<T[K][number]> extends object
            ? `${K}[${number}].${AttributePath<NonNullable<T[K][number]>>}` | `${K}.${AttributePath<NonNullable<T[K][number]>>}`
            : never)
        : NonNullable<T[K]> extends object
        ? K | `${K}.${AttributePath<NonNullable<T[K]>>}`
        : K;
    }[keyof T & string]
  : string;

/**
 * Shared expression builder interface for building DynamoDB expressions.
 * Used by both ConditionExpression and FilterExpression.
 * 
 * @template TEntity - The entity type to infer attribute paths from
 */
export interface ExpressionBuilder<TEntity = unknown> {
  /** Equality comparison */
  eq(attribute: AttributePath<TEntity>, value: unknown): string;
  /** Inequality comparison */
  ne(attribute: AttributePath<TEntity>, value: unknown): string;
  /** Less than comparison */
  lt(attribute: AttributePath<TEntity>, value: unknown): string;
  /** Less than or equal comparison */
  lte(attribute: AttributePath<TEntity>, value: unknown): string;
  /** Greater than comparison */
  gt(attribute: AttributePath<TEntity>, value: unknown): string;
  /** Greater than or equal comparison */
  gte(attribute: AttributePath<TEntity>, value: unknown): string;
  /** Begins with comparison */
  beginsWith(attribute: AttributePath<TEntity>, value: string): string;
  /** 
   * Contains comparison
   * 
   * For strings: checks if the string contains the substring
   * For arrays: checks if the array contains the exact value
   * 
   * Note: For arrays of objects, paths like "metadata.key" are type-checked
   * but may require special handling in the implementation, as DynamoDB's
   * contains() function checks for exact value matches in arrays, not nested
   * property values. To check nested properties in array elements, consider
   * using a filter expression instead.
   */
  contains(attribute: AttributePath<TEntity>, value: string): string;
  /** Between comparison */
  between(attribute: AttributePath<TEntity>, start: unknown, end: unknown): string;
  /** In comparison */
  in(attribute: AttributePath<TEntity>, values: unknown[]): string;
  /** Attribute exists */
  exists(attribute: AttributePath<TEntity>): string;
  /** Attribute does not exist */
  notExists(attribute: AttributePath<TEntity>): string;
  /** Attribute type check */
  attributeType(attribute: AttributePath<TEntity>, type: string): string;
  /** Size comparison */
  size(attribute: AttributePath<TEntity>): string;
  /** Size greater than */
  sizeGt(attribute: AttributePath<TEntity>, value: number): string;
  /** Size less than */
  sizeLt(attribute: AttributePath<TEntity>, value: number): string;
  /** Size greater than or equal */
  sizeGte(attribute: AttributePath<TEntity>, value: number): string;
  /** Size less than or equal */
  sizeLte(attribute: AttributePath<TEntity>, value: number): string;
  /** Logical AND */
  and(...conditions: string[]): string;
  /** Logical OR */
  or(...conditions: string[]): string;
  /** Logical NOT */
  not(condition: string): string;
}

/**
 * Builder for sort key conditions in KeyConditionExpression.
 * This is a subset of ExpressionBuilder, containing only operations valid for sort keys:
 * =, <, <=, >, >=, BETWEEN, begins_with
 * 
 * The attribute parameter is implicit (the sort key field), so methods don't require it.
 */
export interface SortKeyConditionBuilder {
  /** Equality comparison */
  eq(value: unknown): string;
  /** Less than comparison */
  lt(value: unknown): string;
  /** Less than or equal comparison */
  lte(value: unknown): string;
  /** Greater than comparison */
  gt(value: unknown): string;
  /** Greater than or equal comparison */
  gte(value: unknown): string;
  /** Between comparison */
  between(start: unknown, end: unknown): string;
  /** Begins with comparison */
  beginsWith(value: string): string;
}

/**
 * Expression builder with internal state (names and values)
 */
export interface ExpressionBuilderWithState<TEntity = unknown> extends ExpressionBuilder<TEntity> {
  names: Record<string, string>;
  values: Record<string, unknown>;
}

/**
 * Creates a sort key condition builder from an expression builder.
 * The sort key attribute name is bound, so methods don't require it.
 */
export function createSortKeyConditionBuilder<TEntity = unknown>(
  builder: ExpressionBuilder<TEntity>,
  sortKeyAttribute: AttributePath<TEntity>
): SortKeyConditionBuilder {
  const bindAttribute = <T extends unknown[]>(
    method: (attr: AttributePath<TEntity>, ...args: T) => string
  ) => (...args: T) => method(sortKeyAttribute, ...args);

  return {
    eq: bindAttribute(builder.eq),
    lt: bindAttribute(builder.lt),
    lte: bindAttribute(builder.lte),
    gt: bindAttribute(builder.gt),
    gte: bindAttribute(builder.gte),
    between: bindAttribute(builder.between),
    beginsWith: bindAttribute(builder.beginsWith),
  };
}

/**
 * Creates an expression builder function.
 * Returns an object that implements ExpressionBuilder with additional names and values properties.
 */
export function createExpressionBuilder<TEntity = unknown>(): ExpressionBuilderWithState<TEntity> {
  // Internal state encapsulated in closure
  const names: Record<string, string> = {};
  const values: Record<string, unknown> = {};
  let counter = 0;

  const getAttrName = (attr: string): string => {
    const segments = attr.split('.').map((s) => s.trim()).filter(Boolean);
    if (!segments.length) {
      if (!names['#']) names['#'] = '';
      return '#';
    }
    return segments.map((seg) => {
      const key = `#${seg}`;
      if (!names[key]) names[key] = seg;
      return key;
    }).join('.');
  };

  const getValueName = (value: unknown): string => {
    const key = `:val${counter++}`;
    values[key] = value;
    return key;
  };

  const compare = (op: string) => (attr: AttributePath<TEntity>, value: unknown) => {
    const a = getAttrName(attr as string);
    const v = getValueName(value);
    return `${a} ${op} ${v}`;
  };

  const sizeCompare = (op: string) => (attr: AttributePath<TEntity>, value: number) => {
    const a = getAttrName(attr as string);
    const v = getValueName(value);
    return `size(${a}) ${op} ${v}`;
  };

  const logical = (op: string, name: string) => (...conditions: string[]) => {
    if (!conditions.length) throw new TypeError(`${name}() requires at least one condition`);
    return `(${conditions.join(` ${op} `)})`;
  };

  const fn = (funcName: string) => (attr: AttributePath<TEntity>, value: unknown) => {
    const a = getAttrName(attr as string);
    const v = getValueName(value);
    return `${funcName}(${a}, ${v})`;
  };

  const singleAttributeFunction = (funcName: string) => (attr: AttributePath<TEntity>) => {
    const a = getAttrName(attr as string);
    return `${funcName}(${a})`;
  };

  return {
    eq: compare('='),
    ne: compare('<>'),
    lt: compare('<'),
    lte: compare('<='),
    gt: compare('>'),
    gte: compare('>='),
    beginsWith: fn('begins_with'),
    contains: fn('contains'),
    between: (attr: AttributePath<TEntity>, start: unknown, end: unknown) => {
      const a = getAttrName(attr as string);
      const s = getValueName(start);
      const e = getValueName(end);
      return `${a} BETWEEN ${s} AND ${e}`;
    },
    in: (attr: AttributePath<TEntity>, values: unknown[]) => {
      const filtered = values.filter((v) => v !== undefined);
      if (!filtered.length) {
        throw new TypeError(values.length ? 'in() requires at least one non-undefined value' : 'in() requires at least one value');
      }
      const a = getAttrName(attr as string);
      const vs = filtered.map(getValueName);
      return `${a} IN (${vs.join(', ')})`;
    },
    exists: singleAttributeFunction('attribute_exists'),
    notExists: singleAttributeFunction('attribute_not_exists'),
    attributeType: fn('attribute_type'),
    size: singleAttributeFunction('size'),
    sizeGt: sizeCompare('>'),
    sizeLt: sizeCompare('<'),
    sizeGte: sizeCompare('>='),
    sizeLte: sizeCompare('<='),
    and: logical('AND', 'and'),
    or: logical('OR', 'or'),
    not: (condition: string) => {
      const t = condition.trim();
      return (t.startsWith('(') && t.endsWith(')')) ? `NOT ${t}` : `NOT (${condition})`;
    },
    names,
    values,
  };
}

