import { describe, it, expect } from 'vitest';
import { extractKey, validateKeyInput, buildKeyConditionExpression } from '../../src/utils.js';
import type { KeyConfig } from '../../src/types.js';

describe('key-utils', () => {
  describe('extractKey', () => {
    it('should extract partition key only', () => {
      const entity = { id: '123', name: 'John' };
      const keyConfig: KeyConfig = { pk: 'id' };

      const key = extractKey(entity, keyConfig);

      expect(key).toEqual({ id: '123' });
    });

    it('should extract partition and sort keys', () => {
      const entity = { id: '123', email: 'john@example.com', name: 'John' };
      const keyConfig: KeyConfig = { pk: 'id', sk: 'email' };

      const key = extractKey(entity, keyConfig);

      expect(key).toEqual({ id: '123', email: 'john@example.com' });
    });
  });

  describe('extractKey with key input', () => {
    it('should extract key from input object', () => {
      const keyInput = { id: '123' };
      const keyConfig: KeyConfig = { pk: 'id' };

      const key = extractKey(keyInput, keyConfig);

      expect(key).toEqual({ id: '123' });
    });
  });

  describe('validateKeyInput', () => {
    it('should not throw for valid partition key only', () => {
      const keyInput = { id: '123' };
      const keyConfig: KeyConfig = { pk: 'id' };

      expect(() => validateKeyInput(keyInput, keyConfig)).not.toThrow();
    });

    it('should not throw for valid partition and sort keys', () => {
      const keyInput = { id: '123', email: 'john@example.com' };
      const keyConfig: KeyConfig = { pk: 'id', sk: 'email' };

      expect(() => validateKeyInput(keyInput, keyConfig)).not.toThrow();
    });

    it('should throw for missing partition key', () => {
      const keyInput = { name: 'John' };
      const keyConfig: KeyConfig = { pk: 'id' };

      expect(() => validateKeyInput(keyInput, keyConfig)).toThrow(
        'Missing required partition key field: id'
      );
    });

    it('should throw for missing sort key', () => {
      const keyInput = { id: '123' };
      const keyConfig: KeyConfig = { pk: 'id', sk: 'email' };

      expect(() => validateKeyInput(keyInput, keyConfig)).toThrow(
        'Missing required sort key field: email'
      );
    });

    it('should throw for null partition key', () => {
      const keyInput = { id: null };
      const keyConfig: KeyConfig = { pk: 'id' };

      expect(() => validateKeyInput(keyInput, keyConfig)).toThrow();
    });
  });

  describe('buildKeyConditionExpression', () => {
    it('should build expression for partition key only', () => {
      const result = buildKeyConditionExpression('id', 'USER#123');

      expect(result.KeyConditionExpression).toBe('#pk = :pk');
      expect(result.ExpressionAttributeNames).toEqual({ '#pk': 'id' });
      expect(result.ExpressionAttributeValues).toEqual({ ':pk': 'USER#123' });
    });

    it('should build expression with sort key condition', () => {
      const result = buildKeyConditionExpression(
        'id',
        'USER#123',
        'email',
        'begins_with(#sk, :sk_prefix)'
      );

      expect(result.KeyConditionExpression).toBe('#pk = :pk AND begins_with(#sk, :sk_prefix)');
      expect(result.ExpressionAttributeNames).toEqual({ '#pk': 'id', '#sk': 'email' });
      expect(result.ExpressionAttributeValues).toEqual({ ':pk': 'USER#123' });
    });
  });
});
