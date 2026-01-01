import { describe, it, expect, beforeEach } from 'vitest';
import {
  createExpressionBuilder,
  type ExpressionBuilder,
} from './expression';

describe('ExpressionBuilder - Comparison Operations', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;

  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  describe('eq', () => {
    it('should create equality expression', () => {
      const expr = builder.eq('name', 'John');

      expect(expr).toBe('#name = :val0');
      expect(builder.names).toEqual({ '#name': 'name' });
      expect(builder.values).toEqual({ ':val0': 'John' });
    });

    it('should handle numeric values', () => {
      const expr = builder.eq('age', 25);

      expect(expr).toBe('#age = :val0');
      expect(builder.values).toEqual({ ':val0': 25 });
    });

    it('should handle boolean values', () => {
      const expr = builder.eq('active', true);

      expect(expr).toBe('#active = :val0');
      expect(builder.values).toEqual({ ':val0': true });
    });

    it('should handle null values', () => {
      const expr = builder.eq('value', null);

      expect(expr).toBe('#value = :val0');
      expect(builder.values).toEqual({ ':val0': null });
    });

    it('should handle object values', () => {
      const obj = { foo: 'bar' };
      const expr = builder.eq('data', obj);

      expect(expr).toBe('#data = :val0');
      expect(builder.values).toEqual({ ':val0': obj });
    });

    it('should handle array values', () => {
      const arr = [1, 2, 3];
      const expr = builder.eq('items', arr);

      expect(expr).toBe('#items = :val0');
      expect(builder.values).toEqual({ ':val0': arr });
    });
  });

  describe('ne', () => {
    it('should create inequality expression', () => {
      const expr = builder.ne('status', 'inactive');

      expect(expr).toBe('#status <> :val0');
      expect(builder.names).toEqual({ '#status': 'status' });
      expect(builder.values).toEqual({ ':val0': 'inactive' });
    });

    it('should increment counter correctly', () => {
      builder.eq('a', 1);
      const expr = builder.ne('b', 2);

      expect(expr).toBe('#b <> :val1');
      expect(builder.values).toEqual({ ':val0': 1, ':val1': 2 });
    });
  });

  describe('lt', () => {
    it('should create less than expression', () => {
      const expr = builder.lt('age', 18);

      expect(expr).toBe('#age < :val0');
      expect(builder.values).toEqual({ ':val0': 18 });
    });
  });

  describe('lte', () => {
    it('should create less than or equal expression', () => {
      const expr = builder.lte('score', 100);

      expect(expr).toBe('#score <= :val0');
      expect(builder.values).toEqual({ ':val0': 100 });
    });
  });

  describe('gt', () => {
    it('should create greater than expression', () => {
      const expr = builder.gt('price', 50);

      expect(expr).toBe('#price > :val0');
      expect(builder.values).toEqual({ ':val0': 50 });
    });
  });

  describe('gte', () => {
    it('should create greater than or equal expression', () => {
      const expr = builder.gte('quantity', 10);

      expect(expr).toBe('#quantity >= :val0');
      expect(builder.values).toEqual({ ':val0': 10 });
    });
  });
});

describe('ExpressionBuilder - String Operations', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;

  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  describe('beginsWith', () => {
    it('should create begins_with expression', () => {
      const expr = builder.beginsWith('name', 'John');

      expect(expr).toBe('begins_with(#name, :val0)');
      expect(builder.names).toEqual({ '#name': 'name' });
      expect(builder.values).toEqual({ ':val0': 'John' });
    });

    it('should handle empty string', () => {
      const expr = builder.beginsWith('prefix', '');

      expect(expr).toBe('begins_with(#prefix, :val0)');
      expect(builder.values).toEqual({ ':val0': '' });
    });
  });

  describe('contains', () => {
    it('should create contains expression', () => {
      const expr = builder.contains('description', 'important');

      expect(expr).toBe('contains(#description, :val0)');
      expect(builder.names).toEqual({ '#description': 'description' });
      expect(builder.values).toEqual({ ':val0': 'important' });
    });

    it('should handle special characters in value', () => {
      const expr = builder.contains('text', 'test@example.com');

      expect(expr).toBe('contains(#text, :val0)');
      expect(builder.values).toEqual({ ':val0': 'test@example.com' });
    });
  });
});

describe('ExpressionBuilder - Range Operations', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  describe('between', () => {
    it('should create BETWEEN expression', () => {
      const expr = builder.between('age', 18, 65);

      expect(expr).toBe('#age BETWEEN :val0 AND :val1');
      expect(builder.names).toEqual({ '#age': 'age' });
      expect(builder.values).toEqual({ ':val0': 18, ':val1': 65 });
    });

    it('should handle string ranges', () => {
      const expr = builder.between('name', 'A', 'M');

      expect(expr).toBe('#name BETWEEN :val0 AND :val1');
      expect(builder.values).toEqual({ ':val0': 'A', ':val1': 'M' });
    });

    it('should increment counter correctly', () => {
      builder.eq('other', 1);
      const expr = builder.between('value', 10, 20);

      expect(expr).toBe('#value BETWEEN :val1 AND :val2');
      expect(builder.values).toEqual({ ':val0': 1, ':val1': 10, ':val2': 20 });
    });
  });

  describe('in', () => {
    it('should create IN expression with single value', () => {
      const expr = builder.in('status', ['active']);

      expect(expr).toBe('#status IN (:val0)');
      expect(builder.names).toEqual({ '#status': 'status' });
      expect(builder.values).toEqual({ ':val0': 'active' });
    });

    it('should create IN expression with multiple values', () => {
      const expr = builder.in('status', ['active', 'pending', 'completed']);

      expect(expr).toBe('#status IN (:val0, :val1, :val2)');
      expect(builder.values).toEqual({
        ':val0': 'active',
        ':val1': 'pending',
        ':val2': 'completed',
      });
    });

    it('should throw error for empty array', () => {
      expect(() => builder.in('status', [])).toThrow(TypeError);
      expect(() => builder.in('status', [])).toThrow('in() requires at least one value');
    });

    it('should handle mixed types in array', () => {
      const expr = builder.in('value', [1, 'two', true, null]);

      expect(expr).toBe('#value IN (:val0, :val1, :val2, :val3)');
      expect(builder.values).toEqual({
        ':val0': 1,
        ':val1': 'two',
        ':val2': true,
        ':val3': null,
      });
    });

    it('should filter out undefined values from array', () => {
      const expr = builder.in('status', ['active', undefined, 'pending']);

      expect(expr).toBe('#status IN (:val0, :val1)');
      expect(builder.values).toEqual({
        ':val0': 'active',
        ':val1': 'pending',
      });
    });

    it('should throw error if array contains only undefined values', () => {
      expect(() => builder.in('status', [undefined, undefined])).toThrow(TypeError);
      expect(() => builder.in('status', [undefined, undefined])).toThrow('in() requires at least one non-undefined value');
    });

    it('should increment counter correctly', () => {
      builder.eq('other', 1);
      const expr = builder.in('status', ['a', 'b']);

      expect(expr).toBe('#status IN (:val1, :val2)');
      expect(builder.values).toEqual({ ':val0': 1, ':val1': 'a', ':val2': 'b' });
    });
  });
});

describe('ExpressionBuilder - Attribute Existence', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  describe('exists', () => {
    it('should create attribute_exists expression', () => {
      const expr = builder.exists('email');

      expect(expr).toBe('attribute_exists(#email)');
      expect(builder.names).toEqual({ '#email': 'email' });
      expect(builder.values).toEqual({});
    });

    it('should not add values to state', () => {
      builder.exists('field');
      builder.exists('another');

      expect(builder.values).toEqual({});
      expect(Object.keys(builder.names)).toHaveLength(2);
    });

    it('should not add values when using exists', () => {
      const initialValueCount = Object.keys(builder.values).length;
      builder.exists('field');
      expect(Object.keys(builder.values).length).toBe(initialValueCount);
    });
  });

  describe('notExists', () => {
    it('should create attribute_not_exists expression', () => {
      const expr = builder.notExists('deletedAt');

      expect(expr).toBe('attribute_not_exists(#deletedAt)');
      expect(builder.names).toEqual({ '#deletedAt': 'deletedAt' });
      expect(builder.values).toEqual({});
    });

    it('should not add values when using notExists', () => {
      const initialValueCount = Object.keys(builder.values).length;
      builder.notExists('field');
      expect(Object.keys(builder.values).length).toBe(initialValueCount);
    });
  });
});

describe('ExpressionBuilder - Attribute Type', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  describe('attributeType', () => {
    it('should create attribute_type expression', () => {
      const expr = builder.attributeType('data', 'S');

      expect(expr).toBe('attribute_type(#data, :val0)');
      expect(builder.names).toEqual({ '#data': 'data' });
      expect(builder.values).toEqual({ ':val0': 'S' });
    });

    it('should handle different type strings', () => {
      const types = ['S', 'N', 'B', 'SS', 'NS', 'BS', 'M', 'L', 'BOOL', 'NULL'];
      types.forEach((type) => {
        const builder = createExpressionBuilder();
        const expr = builder.attributeType('field', type);

        expect(expr).toContain('attribute_type');
        expect(builder.values[':val0']).toBe(type);
      });
    });

    it('should increment value counter per call', () => {
      builder.attributeType('field1', 'S');
      expect(builder.values[':val0']).toBe('S');
      expect(Object.keys(builder.values).length).toBe(1);

      builder.attributeType('field2', 'N');
      expect(builder.values[':val1']).toBe('N');
      expect(Object.keys(builder.values).length).toBe(2);
    });
  });
});

describe('ExpressionBuilder - Size Operations', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  describe('size', () => {
    it('should create size expression', () => {
      const expr = builder.size('items');

      expect(expr).toBe('size(#items)');
      expect(builder.names).toEqual({ '#items': 'items' });
      expect(builder.values).toEqual({});
    });

    it('should not add values when using size', () => {
      const initialValueCount = Object.keys(builder.values).length;
      builder.size('items');
      expect(Object.keys(builder.values).length).toBe(initialValueCount);
    });
  });

  describe('sizeGt', () => {
    it('should create size greater than expression', () => {
      const expr = builder.sizeGt('items', 5);

      expect(expr).toBe('size(#items) > :val0');
      expect(builder.names).toEqual({ '#items': 'items' });
      expect(builder.values).toEqual({ ':val0': 5 });
    });

    it('should handle zero', () => {
      const expr = builder.sizeGt('list', 0);

      expect(expr).toBe('size(#list) > :val0');
      expect(builder.values).toEqual({ ':val0': 0 });
    });
  });

  describe('sizeLt', () => {
    it('should create size less than expression', () => {
      const expr = builder.sizeLt('tags', 10);

      expect(expr).toBe('size(#tags) < :val0');
      expect(builder.names).toEqual({ '#tags': 'tags' });
      expect(builder.values).toEqual({ ':val0': 10 });
    });
  });

  describe('sizeGte', () => {
    it('should create size greater than or equal expression', () => {
      const expr = builder.sizeGte('array', 1);

      expect(expr).toBe('size(#array) >= :val0');
      expect(builder.values).toEqual({ ':val0': 1 });
    });
  });

  describe('sizeLte', () => {
    it('should create size less than or equal expression', () => {
      const expr = builder.sizeLte('collection', 100);

      expect(expr).toBe('size(#collection) <= :val0');
      expect(builder.values).toEqual({ ':val0': 100 });
    });
  });
});

describe('ExpressionBuilder - Logical Operations', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  describe('and', () => {
    it('should create AND expression with two conditions', () => {
      const cond1 = builder.eq('status', 'active');
      const cond2 = builder.gt('age', 18);
      const expr = builder.and(cond1, cond2);

      expect(expr).toBe('(#status = :val0 AND #age > :val1)');
    });

    it('should throw error for zero arguments', () => {
      expect(() => builder.and()).toThrow(TypeError);
      expect(() => builder.and()).toThrow('and() requires at least one condition');
    });

    it('should create AND expression with single condition', () => {
      const cond1 = builder.eq('name', 'John');
      const expr = builder.and(cond1);

      expect(expr).toBe('(#name = :val0)');
    });

    it('should create AND expression with multiple conditions', () => {
      const cond1 = builder.eq('a', 1);
      const cond2 = builder.eq('b', 2);
      const cond3 = builder.eq('c', 3);
      const expr = builder.and(cond1, cond2, cond3);

      expect(expr).toBe('(#a = :val0 AND #b = :val1 AND #c = :val2)');
    });

    it('should handle nested AND expressions', () => {
      const cond1 = builder.eq('a', 1);
      const cond2 = builder.eq('b', 2);
      const nested = builder.and(cond1, cond2);
      const cond3 = builder.eq('c', 3);
      const expr = builder.and(nested, cond3);

      expect(expr).toBe('((#a = :val0 AND #b = :val1) AND #c = :val2)');
    });
  });

  describe('or', () => {
    it('should create OR expression with two conditions', () => {
      const cond1 = builder.eq('status', 'active');
      const cond2 = builder.eq('status', 'pending');
      const expr = builder.or(cond1, cond2);

      expect(expr).toBe('(#status = :val0 OR #status = :val1)');
    });

    it('should throw error for zero arguments', () => {
      expect(() => builder.or()).toThrow(TypeError);
      expect(() => builder.or()).toThrow('or() requires at least one condition');
    });

    it('should create OR expression with multiple conditions', () => {
      const cond1 = builder.eq('type', 'A');
      const cond2 = builder.eq('type', 'B');
      const cond3 = builder.eq('type', 'C');
      const expr = builder.or(cond1, cond2, cond3);

      expect(expr).toBe('(#type = :val0 OR #type = :val1 OR #type = :val2)');
    });

    it('should handle nested OR expressions', () => {
      const cond1 = builder.eq('a', 1);
      const cond2 = builder.eq('b', 2);
      const nested = builder.or(cond1, cond2);
      const cond3 = builder.eq('c', 3);
      const expr = builder.or(nested, cond3);

      expect(expr).toBe('((#a = :val0 OR #b = :val1) OR #c = :val2)');
    });
  });

  describe('not', () => {
    it('should create NOT expression', () => {
      const cond = builder.eq('deleted', true);
      const expr = builder.not(cond);

      expect(expr).toBe('NOT (#deleted = :val0)');
    });

    it('should handle NOT with complex expressions', () => {
      const cond1 = builder.eq('a', 1);
      const cond2 = builder.eq('b', 2);
      const andExpr = builder.and(cond1, cond2);
      const expr = builder.not(andExpr);

      expect(expr).toBe('NOT (#a = :val0 AND #b = :val1)');
    });

    it('should avoid double parentheses when condition is already parenthesized', () => {
      const cond = builder.eq('status', 'active');
      const parenthesized = `(${cond})`;
      const expr = builder.not(parenthesized);

      expect(expr).toBe('NOT (#status = :val0)');
    });

    it('should add parentheses when condition is not parenthesized', () => {
      const cond = builder.eq('status', 'active');
      const expr = builder.not(cond);

      expect(expr).toBe('NOT (#status = :val0)');
    });
  });
});

describe('ExpressionBuilder - Complex Scenarios', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  it('should handle same attribute name used multiple times', () => {
    const expr1 = builder.eq('status', 'active');
    const expr2 = builder.eq('status', 'pending');

    expect(expr1).toBe('#status = :val0');
    expect(expr2).toBe('#status = :val1');
    expect(builder.names).toEqual({ '#status': 'status' });
    expect(builder.values).toEqual({ ':val0': 'active', ':val1': 'pending' });
  });

  it('should reuse same attribute name across different operations', () => {
    const expr1 = builder.eq('age', 18);
    const expr2 = builder.gt('age', 21);

    expect(expr1).toBe('#age = :val0');
    expect(expr2).toBe('#age > :val1');
    expect(builder.names).toEqual({ '#age': 'age' });
    expect(Object.keys(builder.names)).toHaveLength(1);
  });

  it('should handle complex nested logical expressions', () => {
    const ageCheck = builder.gte('age', 18);
    const statusCheck = builder.eq('status', 'active');
    const orExpr = builder.or(ageCheck, statusCheck);
    const nameCheck = builder.eq('name', 'John');
    const finalExpr = builder.and(orExpr, nameCheck);

    expect(finalExpr).toBe(
      '((#age >= :val0 OR #status = :val1) AND #name = :val2)',
    );
    expect(builder.values).toEqual({
      ':val0': 18,
      ':val1': 'active',
      ':val2': 'John',
    });
  });

  it('should tokenize attribute paths by dots', () => {
    const expr = builder.eq('user.email', 'test@example.com');

    expect(expr).toBe('#user.#email = :val0');
    expect(builder.names).toEqual({ '#user': 'user', '#email': 'email' });
    expect(builder.values).toEqual({ ':val0': 'test@example.com' });
  });

  it('should handle nested attribute paths', () => {
    const expr = builder.eq('user.profile.name', 'John');

    expect(expr).toBe('#user.#profile.#name = :val0');
    expect(builder.names).toEqual({
      '#user': 'user',
      '#profile': 'profile',
      '#name': 'name',
    });
  });

  it('should handle array index paths', () => {
    const expr = builder.eq('items[0].name', 'Item 1');

    expect(expr).toBe('#items[0].#name = :val0');
    expect(builder.names).toEqual({
      '#items[0]': 'items[0]',
      '#name': 'name',
    });
  });

  it('should reuse same path segments across operations', () => {
    builder.eq('user.email', 'test@example.com');
    builder.eq('user.name', 'John');

    expect(builder.names).toEqual({
      '#user': 'user',
      '#email': 'email',
      '#name': 'name',
    });
    expect(Object.keys(builder.names)).toHaveLength(3);
  });

  it('should handle attribute names with dashes', () => {
    const expr = builder.eq('user-name', 'John Doe');

    expect(expr).toBe('#user-name = :val0');
    expect(builder.names).toEqual({ '#user-name': 'user-name' });
  });

  it('should handle attribute names with underscores', () => {
    const expr = builder.eq('user_name', 'john_doe');

    expect(expr).toBe('#user_name = :val0');
    expect(builder.names).toEqual({ '#user_name': 'user_name' });
  });

  it('should handle numeric attribute names', () => {
    const expr = builder.eq('field1', 'value');

    expect(expr).toBe('#field1 = :val0');
    expect(builder.names).toEqual({ '#field1': 'field1' });
  });

  it('should maintain correct counter across multiple operations', () => {
    builder.eq('a', 1);
    builder.ne('b', 2);
    builder.lt('c', 3);
    builder.gt('d', 4);
    builder.between('e', 5, 6);
    builder.in('f', [7, 8, 9]);

    expect(Object.keys(builder.values)).toEqual([
      ':val0',
      ':val1',
      ':val2',
      ':val3',
      ':val4',
      ':val5',
      ':val6',
      ':val7',
      ':val8',
    ]);
    expect(builder.values).toEqual({
      ':val0': 1,
      ':val1': 2,
      ':val2': 3,
      ':val3': 4,
      ':val4': 5,
      ':val5': 6,
      ':val6': 7,
      ':val7': 8,
      ':val8': 9,
    });
  });

  it('should handle all operations together in a complex query', () => {
    const eqExpr = builder.eq('status', 'active');
    const neExpr = builder.ne('deleted', true);
    const ltExpr = builder.lt('age', 65);
    const gteExpr = builder.gte('score', 0);
    const beginsExpr = builder.beginsWith('name', 'A');
    const containsExpr = builder.contains('description', 'important');
    const betweenExpr = builder.between('price', 10, 100);
    const inExpr = builder.in('category', ['A', 'B', 'C']);
    const existsExpr = builder.exists('email');
    const notExistsExpr = builder.notExists('deletedAt');
    const sizeExpr = builder.sizeGt('tags', 0);

    const combined = builder.and(
      eqExpr,
      neExpr,
      ltExpr,
      gteExpr,
      beginsExpr,
      containsExpr,
      betweenExpr,
      inExpr,
      existsExpr,
      notExistsExpr,
      sizeExpr,
    );

    expect(combined).toContain('#status = :val0');
    expect(combined).toContain('#deleted <> :val1');
    expect(combined).toContain('#age < :val2');
    expect(combined).toContain('#score >= :val3');
    expect(combined).toContain('begins_with(#name, :val4)');
    expect(combined).toContain('contains(#description, :val5)');
    expect(combined).toContain('#price BETWEEN :val6 AND :val7');
    expect(combined).toContain('#category IN (:val8, :val9, :val10)');
    expect(combined).toContain('attribute_exists(#email)');
    expect(combined).toContain('attribute_not_exists(#deletedAt)');
    expect(combined).toContain('size(#tags) > :val11');
  });

  it('should handle deeply nested logical expressions', () => {
    const a = builder.eq('a', 1);
    const b = builder.eq('b', 2);
    const c = builder.eq('c', 3);
    const d = builder.eq('d', 4);

    const level1 = builder.and(a, b);
    const level2 = builder.or(c, d);
    const level3 = builder.and(level1, level2);
    const final = builder.not(level3);

    expect(final).toBe(
      'NOT ((#a = :val0 AND #b = :val1) AND (#c = :val2 OR #d = :val3))',
    );
  });
});

describe('ExpressionBuilder - State Isolation', () => {
  it('should maintain separate state for different builders', () => {
    const builder1 = createExpressionBuilder();
    const builder2 = createExpressionBuilder();

    builder1.eq('field', 'value1');
    builder2.eq('field', 'value2');

    expect(builder1.values).toEqual({ ':val0': 'value1' });
    expect(builder2.values).toEqual({ ':val0': 'value2' });
    expect(builder1.values).not.toBe(builder2.values);
    expect(builder1.names).not.toBe(builder2.names);
  });

  it('should maintain independent state for each builder instance', () => {
    const builder1 = createExpressionBuilder();
    const builder2 = createExpressionBuilder();

    const expr1 = builder1.eq('a', 1);
    const expr2 = builder2.eq('b', 2);

    expect(expr1).toBe('#a = :val0');
    expect(expr2).toBe('#b = :val0');
    expect(builder1.values).toEqual({ ':val0': 1 });
    expect(builder2.values).toEqual({ ':val0': 2 });
    expect(builder1.names).toEqual({ '#a': 'a' });
    expect(builder2.names).toEqual({ '#b': 'b' });
  });

  it('should maintain separate attribute name mappings for each builder', () => {
    const builder1 = createExpressionBuilder();
    const builder2 = createExpressionBuilder();

    builder1.eq('status', 'active');
    builder2.gt('status', 0);

    expect(builder1.names).toEqual({ '#status': 'status' });
    expect(builder2.names).toEqual({ '#status': 'status' });
    expect(builder1.names).not.toBe(builder2.names);
  });
});

describe('ExpressionBuilder - Edge Cases', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  it('should handle empty string attribute names', () => {
    const expr = builder.eq('', 'value');

    expect(expr).toBe('# = :val0');
    expect(builder.names).toEqual({ '#': '' });
  });

  it('should handle very long attribute names', () => {
    const longName = 'a'.repeat(100);
    const expr = builder.eq(longName, 'value');

    expect(expr).toBe(`#${longName} = :val0`);
    expect(builder.names[`#${longName}`]).toBe(longName);
  });

  it('should handle undefined values', () => {
    const expr = builder.eq('field', undefined);

    expect(expr).toBe('#field = :val0');
    expect(builder.values).toEqual({ ':val0': undefined });
  });

  it('should handle NaN values', () => {
    const expr = builder.eq('field', NaN);

    expect(expr).toBe('#field = :val0');
    expect(builder.values).toEqual({ ':val0': NaN });
  });

  it('should handle Infinity values', () => {
    const expr = builder.eq('field', Infinity);

    expect(expr).toBe('#field = :val0');
    expect(builder.values).toEqual({ ':val0': Infinity });
  });

  it('should handle negative Infinity values', () => {
    const expr = builder.eq('field', -Infinity);

    expect(expr).toBe('#field = :val0');
    expect(builder.values).toEqual({ ':val0': -Infinity });
  });

  it('should handle Date objects', () => {
    const date = new Date('2024-01-01');
    const expr = builder.eq('createdAt', date);

    expect(expr).toBe('#createdAt = :val0');
    expect(builder.values).toEqual({ ':val0': date });
  });

  it('should handle RegExp objects', () => {
    const regex = /test/gi;
    const expr = builder.eq('pattern', regex);

    expect(expr).toBe('#pattern = :val0');
    expect(builder.values).toEqual({ ':val0': regex });
  });

  it('should handle circular references in objects', () => {
    const obj: any = { name: 'test' };
    obj.self = obj;
    const expr = builder.eq('data', obj);

    expect(expr).toBe('#data = :val0');
    expect(builder.values[':val0']).toBe(obj);
  });
});

describe('ExpressionBuilder - Counter Behavior', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  it('should not add values for exists, notExists, and size', () => {
    const initialValueCount = Object.keys(builder.values).length;
    
    builder.exists('field1');
    expect(Object.keys(builder.values).length).toBe(initialValueCount);
    
    builder.notExists('field2');
    expect(Object.keys(builder.values).length).toBe(initialValueCount);
    
    builder.size('field3');
    expect(Object.keys(builder.values).length).toBe(initialValueCount);
  });

  it('should add values for attributeType', () => {
    builder.attributeType('field', 'S');
    expect(Object.keys(builder.values).length).toBe(1);
    expect(builder.values[':val0']).toBe('S');
    
    builder.attributeType('field2', 'N');
    expect(Object.keys(builder.values).length).toBe(2);
    expect(builder.values[':val1']).toBe('N');
  });

  it('should increment value counter sequentially across all operations', () => {
    builder.eq('a', 1); // :val0
    builder.exists('b'); // no increment
    builder.attributeType('c', 'S'); // :val1
    builder.notExists('d'); // no increment
    builder.size('e'); // no increment
    builder.attributeType('f', 'N'); // :val2
    builder.gt('g', 3); // :val3

    expect(Object.keys(builder.values)).toEqual([':val0', ':val1', ':val2', ':val3']);
    expect(Object.keys(builder.values).length).toBe(4);
  });
});

describe('ExpressionBuilder - Reserved Words', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  it('should handle reserved words as attribute names', () => {
    const reservedWords = ['size', 'name', 'status', 'type', 'value', 'data'];
    
    reservedWords.forEach((word) => {
      const expr = builder.eq(word, 'test');
      expect(expr).toContain(`#${word}`);
      expect(builder.names[`#${word}`]).toBe(word);
    });
  });

  it('should use placeholders for reserved words', () => {
    builder.eq('size', 10);
    builder.size('items');
    
    // 'size' as attribute should use #size, and size() function should use #items
    expect(builder.names['#size']).toBe('size');
    expect(builder.names['#items']).toBe('items');
  });
});

describe('ExpressionBuilder - Non-Primitive Comparisons', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  it('should allow string comparisons with lt/lte/gt/gte', () => {
    const expr1 = builder.lt('name', 'Z');
    const expr2 = builder.gte('name', 'A');
    
    expect(expr1).toBe('#name < :val0');
    expect(expr2).toBe('#name >= :val1');
    expect(builder.values[':val0']).toBe('Z');
    expect(builder.values[':val1']).toBe('A');
  });

  it('should allow boolean comparisons with lt/lte/gt/gte', () => {
    const expr = builder.gt('flag', false);
    
    expect(expr).toBe('#flag > :val0');
    expect(builder.values[':val0']).toBe(false);
  });

  it('should allow null comparisons with lt/lte/gt/gte', () => {
    const expr = builder.lte('value', null);
    
    expect(expr).toBe('#value <= :val0');
    expect(builder.values[':val0']).toBe(null);
  });
});

describe('ExpressionBuilder - Reversed Between Bounds', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  it('should allow reversed bounds in between', () => {
    const expr = builder.between('age', 65, 18);
    
    expect(expr).toBe('#age BETWEEN :val0 AND :val1');
    expect(builder.values[':val0']).toBe(65);
    expect(builder.values[':val1']).toBe(18);
  });

  it('should allow reversed string bounds', () => {
    const expr = builder.between('name', 'Z', 'A');
    
    expect(expr).toBe('#name BETWEEN :val0 AND :val1');
    expect(builder.values[':val0']).toBe('Z');
    expect(builder.values[':val1']).toBe('A');
  });
});

describe('ExpressionBuilder - Complex Real-World Query', () => {
  let builder: ReturnType<typeof createExpressionBuilder>;
  beforeEach(() => {
    builder = createExpressionBuilder();
  });

  it('should build a complex query with nested paths, multiple operations, and logical combinations', () => {
    // Build a complex query: Find active users with verified email, 
    // age between 18-65, score >= 50, with orders containing specific items,
    // and either premium status or high activity level

    // Basic user conditions
    const isActive = builder.eq('user.status', 'active');
    const hasEmail = builder.exists('user.profile.email');
    const emailVerified = builder.eq('user.profile.emailVerified', true);
    const ageRange = builder.between('user.profile.age', 18, 65);
    const minScore = builder.gte('user.score', 50);
    const notDeleted = builder.notExists('user.deletedAt');

    // Profile conditions
    const emailStartsWith = builder.beginsWith('user.profile.email', 'user@');
    const nameContains = builder.contains('user.profile.name', 'John');
    const validStatus = builder.in('user.status', ['active', 'pending', 'verified']);

    // Order conditions
    const hasOrders = builder.exists('user.orders');
    const orderCount = builder.sizeGt('user.orders', 0);
    const firstOrderName = builder.eq('user.orders[0].name', 'Premium');
    const orderInRange = builder.between('user.orders[0].total', 100, 1000);

    // Complex nested logical conditions
    const profileConditions = builder.and(
      isActive,
      hasEmail,
      emailVerified,
      ageRange,
      minScore,
      notDeleted,
    );

    const emailConditions = builder.and(
      emailStartsWith,
      nameContains,
    );

    const orderConditions = builder.and(
      hasOrders,
      orderCount,
      firstOrderName,
      orderInRange,
    );

    // Premium or high activity
    const isPremium = builder.eq('user.membership.type', 'premium');
    const highActivity = builder.gte('user.activity.score', 90);
    const premiumOrActive = builder.or(isPremium, highActivity);

    // Status validation
    const statusCheck = builder.and(validStatus);

    // Combine everything: (profile AND email) AND (orders OR premium/active) AND status
    const emailAndProfile = builder.and(profileConditions, emailConditions);
    const ordersOrPremium = builder.or(orderConditions, premiumOrActive);
    const finalQuery = builder.and(emailAndProfile, ordersOrPremium, statusCheck);

    // Verify the expression structure
    expect(finalQuery).toContain('#user.#status');
    expect(finalQuery).toContain('#user.#profile.#email');
    expect(finalQuery).toContain('#user.#profile.#emailVerified');
    expect(finalQuery).toContain('#user.#profile.#age');
    expect(finalQuery).toContain('#user.#score');
    expect(finalQuery).toContain('#user.#deletedAt');
    expect(finalQuery).toContain('#user.#profile.#name');
    expect(finalQuery).toContain('#user.#orders');
    expect(finalQuery).toContain('#user.#orders[0].#name');
    expect(finalQuery).toContain('#user.#orders[0].#total');
    expect(finalQuery).toContain('#user.#membership.#type');
    expect(finalQuery).toContain('#user.#activity.#score');

    // Verify all operations are present
    expect(finalQuery).toContain('=');
    expect(finalQuery).toContain('BETWEEN');
    expect(finalQuery).toContain('IN');
    expect(finalQuery).toContain('begins_with');
    expect(finalQuery).toContain('contains');
    expect(finalQuery).toContain('attribute_exists');
    expect(finalQuery).toContain('attribute_not_exists');
    expect(finalQuery).toContain('size');
    expect(finalQuery).toContain('>');
    expect(finalQuery).toContain('>=');
    expect(finalQuery).toContain('AND');
    expect(finalQuery).toContain('OR');

    // Verify attribute name reuse (user.status used multiple times)
    const statusKeys = Object.keys(builder.names).filter((key) => builder.names[key] === 'status');
    expect(statusKeys.length).toBeGreaterThan(0);

    // Verify all segments are properly tokenized
    expect(builder.names['#user']).toBe('user');
    expect(builder.names['#profile']).toBe('profile');
    expect(builder.names['#email']).toBe('email');
    expect(builder.names['#orders[0]']).toBe('orders[0]');

    // Verify counter increments correctly (should have many values)
    const valueCount = Object.keys(builder.values).length;
    expect(valueCount).toBeGreaterThan(10);

    // Verify the query is properly parenthesized
    expect(finalQuery.startsWith('(')).toBe(true);
    expect(finalQuery.endsWith(')')).toBe(true);

    // Verify no double parentheses in NOT operations
    expect(finalQuery).not.toContain('NOT ((');

    expect(finalQuery).toMatchInlineSnapshot(`"(((#user.#status = :val0 AND attribute_exists(#user.#profile.#email) AND #user.#profile.#emailVerified = :val1 AND #user.#profile.#age BETWEEN :val2 AND :val3 AND #user.#score >= :val4 AND attribute_not_exists(#user.#deletedAt)) AND (begins_with(#user.#profile.#email, :val5) AND contains(#user.#profile.#name, :val6))) AND ((attribute_exists(#user.#orders) AND size(#user.#orders) > :val10 AND #user.#orders[0].#name = :val11 AND #user.#orders[0].#total BETWEEN :val12 AND :val13) OR (#user.#membership.#type = :val14 OR #user.#activity.#score >= :val15)) AND (#user.#status IN (:val7, :val8, :val9)))"`)
  });

  it('should handle an extremely complex nested query with multiple NOT operations', () => {
    // Build: NOT (user is deleted OR (status is inactive AND score < 50)) 
    // AND (email exists OR phone exists) 
    // AND (age >= 18 AND (premium OR (orders > 0 AND total > 100)))

    const isDeleted = builder.eq('user.deleted', true);
    const isInactive = builder.eq('user.status', 'inactive');
    const lowScore = builder.lt('user.score', 50);
    const hasEmail = builder.exists('user.email');
    const hasPhone = builder.exists('user.phone');
    const minAge = builder.gte('user.age', 18);
    const isPremium = builder.eq('user.membership', 'premium');
    const hasOrders = builder.sizeGt('user.orders', 0);
    const minTotal = builder.gt('user.total', 100);

    // NOT (deleted OR (inactive AND lowScore))
    const inactiveAndLow = builder.and(isInactive, lowScore);
    const deletedOrBad = builder.or(isDeleted, inactiveAndLow);
    const notDeletedOrBad = builder.not(deletedOrBad);

    // (email OR phone)
    const contactExists = builder.or(hasEmail, hasPhone);

    // (premium OR (orders AND total))
    const ordersAndTotal = builder.and(hasOrders, minTotal);
    const premiumOrActive = builder.or(isPremium, ordersAndTotal);

    // age AND (premium OR active)
    const ageAndActivity = builder.and(minAge, premiumOrActive);

    // Final: NOT (deleted OR bad) AND contact AND (age AND activity)
    const finalQuery = builder.and(notDeletedOrBad, contactExists, ageAndActivity);

    // Verify structure
    expect(finalQuery).toContain('NOT');
    expect(finalQuery).toContain('#user.#deleted');
    expect(finalQuery).toContain('#user.#status');
    expect(finalQuery).toContain('#user.#score');
    expect(finalQuery).toContain('#user.#email');
    expect(finalQuery).toContain('#user.#phone');
    expect(finalQuery).toContain('#user.#age');
    expect(finalQuery).toContain('#user.#membership');
    expect(finalQuery).toContain('#user.#orders');
    expect(finalQuery).toContain('#user.#total');

    // Verify NOT doesn't have double parentheses
    const notIndex = finalQuery.indexOf('NOT');
    const afterNot = finalQuery.substring(notIndex + 3).trim();
    expect(afterNot.startsWith('(')).toBe(true);
    expect(afterNot).not.toContain('((');

    // Verify all logical operators
    expect(finalQuery.split('AND').length).toBeGreaterThan(2);
    expect(finalQuery.split('OR').length).toBeGreaterThan(1);
  });
});

