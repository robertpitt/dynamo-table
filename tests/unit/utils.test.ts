import { describe, it, expect } from 'vitest';
import {
  extractKey,
  validateKeyInput,
  extractAndValidateKey,
  buildKeyConditionExpression,
  buildUpdateExpression,
  normalizeExpressionAttributes,
  mergeAndNormalize,
  mergeExpressionAttributes,
  encodePageKey,
  decodePageKey,
  toPagedResult,
  mapBatchWriteRequests,
  mapTransactItems,
  sortBatchResults,
  sortKeyConditions,
} from '../../src/utils.js';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import type { QueryCommandOutput, ScanCommandOutput } from '@aws-sdk/client-dynamodb';

describe('utils', () => {
  describe('extractKey', () => {
    it('should extract partition key only', () => {
      const entity = { id: 'test-id', name: 'Test', email: 'test@example.com' };
      const keyConfig = { pk: 'id' };
      const result = extractKey(entity, keyConfig);

      expect(result).toEqual({ id: 'test-id' });
    });

    it('should extract composite key', () => {
      const entity = { id: 'test-id', email: 'test@example.com', name: 'Test' };
      const keyConfig = { pk: 'id', sk: 'email' };
      const result = extractKey(entity, keyConfig);

      expect(result).toEqual({ id: 'test-id', email: 'test@example.com' });
    });

    it('should handle undefined sort key', () => {
      const entity = { id: 'test-id', name: 'Test' };
      const keyConfig = { pk: 'id', sk: undefined };
      const result = extractKey(entity, keyConfig);

      expect(result).toEqual({ id: 'test-id' });
    });
  });

  describe('validateKeyInput', () => {
    it('should validate partition key exists', () => {
      const keyInput = { id: 'test-id' };
      const keyConfig = { pk: 'id' };

      expect(() => validateKeyInput(keyInput, keyConfig)).not.toThrow();
    });

    it('should throw when partition key is missing', () => {
      const keyInput = { name: 'Test' };
      const keyConfig = { pk: 'id' };

      expect(() => validateKeyInput(keyInput as any, keyConfig)).toThrow(
        'Missing required partition key field: id'
      );
    });

    it('should throw when partition key is null', () => {
      const keyInput = { id: null };
      const keyConfig = { pk: 'id' };

      expect(() => validateKeyInput(keyInput as any, keyConfig)).toThrow(
        'Missing required partition key field: id'
      );
    });

    it('should validate composite key', () => {
      const keyInput = { id: 'test-id', email: 'test@example.com' };
      const keyConfig = { pk: 'id', sk: 'email' };

      expect(() => validateKeyInput(keyInput, keyConfig)).not.toThrow();
    });

    it('should throw when sort key is missing', () => {
      const keyInput = { id: 'test-id' };
      const keyConfig = { pk: 'id', sk: 'email' };

      expect(() => validateKeyInput(keyInput as any, keyConfig)).toThrow(
        'Missing required sort key field: email'
      );
    });
  });

  describe('extractAndValidateKey', () => {
    it('should extract and validate key', () => {
      const keyInput = { id: 'test-id', email: 'test@example.com' };
      const keyConfig = { pk: 'id', sk: 'email' };
      const result = extractAndValidateKey(keyInput, keyConfig);

      expect(result).toEqual({ id: 'test-id', email: 'test@example.com' });
    });

    it('should throw when validation fails', () => {
      const keyInput = { id: 'test-id' };
      const keyConfig = { pk: 'id', sk: 'email' };

      expect(() => extractAndValidateKey(keyInput as any, keyConfig)).toThrow();
    });
  });

  describe('buildKeyConditionExpression', () => {
    it('should build expression with partition key only', () => {
      const result = buildKeyConditionExpression('id', 'test-id');

      expect(result.KeyConditionExpression).toBe('#pk = :pk');
      expect(result.ExpressionAttributeNames).toEqual({ '#pk': 'id' });
      expect(result.ExpressionAttributeValues).toEqual({ ':pk': 'test-id' });
    });

    it('should build expression with sort key condition', () => {
      const result = buildKeyConditionExpression('id', 'test-id', 'email', '#sk = :sk');

      expect(result.KeyConditionExpression).toBe('#pk = :pk AND #sk = :sk');
      expect(result.ExpressionAttributeNames).toEqual({ '#pk': 'id', '#sk': 'email' });
      expect(result.ExpressionAttributeValues).toEqual({ ':pk': 'test-id' });
    });

    it('should handle sort key condition without sort key field', () => {
      const result = buildKeyConditionExpression('id', 'test-id', undefined, '#sk = :sk');

      expect(result.KeyConditionExpression).toBe('#pk = :pk');
    });
  });

  describe('buildUpdateExpression', () => {
    it('should build SET expression for non-null values', () => {
      const updates = { name: 'New Name', email: 'new@example.com' };
      const result = buildUpdateExpression(updates);

      expect(result.UpdateExpression).toContain('SET');
      expect(result.UpdateExpression).toContain('#name = :name');
      expect(result.UpdateExpression).toContain('#email = :email');
      expect(result.ExpressionAttributeNames).toEqual({ '#name': 'name', '#email': 'email' });
      expect(result.ExpressionAttributeValues).toEqual({
        ':name': 'New Name',
        ':email': 'new@example.com',
      });
    });

    it('should build REMOVE expression for null values', () => {
      const updates = { name: null, email: 'test@example.com' };
      const result = buildUpdateExpression(updates);

      expect(result.UpdateExpression).toContain('REMOVE');
      expect(result.UpdateExpression).toContain('#name');
      expect(result.UpdateExpression).toContain('SET');
      expect(result.UpdateExpression).toContain('#email = :email');
      expect(result.ExpressionAttributeNames).toEqual({ '#name': 'name', '#email': 'email' });
      expect(result.ExpressionAttributeValues).toEqual({ ':email': 'test@example.com' });
    });

    it('should handle mixed SET and REMOVE', () => {
      const updates = { name: 'New Name', email: null };
      const result = buildUpdateExpression(updates);

      expect(result.UpdateExpression).toContain('SET');
      expect(result.UpdateExpression).toContain('REMOVE');
      expect(result.ExpressionAttributeNames).toEqual({ '#name': 'name', '#email': 'email' });
      expect(result.ExpressionAttributeValues).toEqual({ ':name': 'New Name' });
    });

    it('should handle undefined values as SET operations', () => {
      const updates = { name: 'New Name', email: undefined };
      const result = buildUpdateExpression(updates);

      expect(result.UpdateExpression).toContain('SET');
      expect(result.UpdateExpression).toContain('#name = :name');
      expect(result.ExpressionAttributeValues).toEqual({ ':name': 'New Name' });
    });

    it('should return empty expression for empty updates', () => {
      const updates = {};
      const result = buildUpdateExpression(updates);

      expect(result.UpdateExpression).toBe('');
      expect(result.ExpressionAttributeNames).toEqual({});
      expect(result.ExpressionAttributeValues).toEqual({});
    });
  });

  describe('normalizeExpressionAttributes', () => {
    it('should return attributes when not empty', () => {
      const attrs = { '#name': 'name', '#email': 'email' };
      const result = normalizeExpressionAttributes(attrs);

      expect(result).toEqual(attrs);
    });

    it('should return undefined when empty', () => {
      const attrs = {};
      const result = normalizeExpressionAttributes(attrs);

      expect(result).toBeUndefined();
    });
  });

  describe('mergeAndNormalize', () => {
    it('should merge multiple attribute objects', () => {
      const attrs1 = { '#name': 'name' };
      const attrs2 = { '#email': 'email' };
      const result = mergeAndNormalize(attrs1, attrs2);

      expect(result).toEqual({ '#name': 'name', '#email': 'email' });
    });

    it('should return undefined when merged result is empty', () => {
      const result = mergeAndNormalize({}, undefined);

      expect(result).toBeUndefined();
    });

    it('should handle undefined inputs', () => {
      const attrs1 = { '#name': 'name' };
      const result = mergeAndNormalize(attrs1, undefined);

      expect(result).toEqual({ '#name': 'name' });
    });

    it('should override with later values', () => {
      const attrs1 = { '#name': 'name1' };
      const attrs2 = { '#name': 'name2' };
      const result = mergeAndNormalize(attrs1, attrs2);

      expect(result).toEqual({ '#name': 'name2' });
    });
  });

  describe('mergeExpressionAttributes', () => {
    it('should merge multiple attribute objects', () => {
      const attrs1 = { '#name': 'name' };
      const attrs2 = { '#email': 'email' };
      const result = mergeExpressionAttributes(attrs1, attrs2);

      expect(result).toEqual({ '#name': 'name', '#email': 'email' });
    });

    it('should handle undefined inputs', () => {
      const attrs1 = { '#name': 'name' };
      const result = mergeExpressionAttributes(attrs1, undefined);

      expect(result).toEqual({ '#name': 'name' });
    });

    it('should return empty object when all inputs are undefined', () => {
      const result = mergeExpressionAttributes(undefined, undefined);

      expect(result).toEqual({});
    });
  });

  describe('encodePageKey / decodePageKey', () => {
    it('should encode and decode page key', () => {
      const key = { id: 'test-id', email: 'test@example.com' };
      const encoded = encodePageKey(key);
      const decoded = decodePageKey(encoded);

      expect(decoded).toEqual(key);
    });

    it('should handle simple keys', () => {
      const key = { id: 'test-id' };
      const encoded = encodePageKey(key);
      const decoded = decodePageKey(encoded);

      expect(decoded).toEqual(key);
    });

    it('should produce valid base64 strings', () => {
      const key = { id: 'test-id' };
      const encoded = encodePageKey(key);

      expect(encoded).toBeTruthy();
      expect(typeof encoded).toBe('string');
      // Base64 strings should only contain valid characters
      expect(encoded).toMatch(/^[A-Za-z0-9+/=]+$/);
    });
  });

  describe('toPagedResult', () => {
    it('should convert query result to paged result', () => {
      const items = [
        { id: '1', name: 'Item 1' },
        { id: '2', name: 'Item 2' },
      ];
      const result: QueryCommandOutput = {
        Items: items.map((item) => marshall(item)),
        LastEvaluatedKey: undefined,
        $metadata: {},
      };

      const paged = toPagedResult(result);

      expect(paged.items).toHaveLength(2);
      expect(paged.items[0]).toEqual(items[0]);
      expect(paged.items[1]).toEqual(items[1]);
      expect(paged.hasNextPage).toBe(false);
      expect(paged.nextPageKey).toBeUndefined();
    });

    it('should handle pagination with LastEvaluatedKey', () => {
      const items = [{ id: '1', name: 'Item 1' }];
      const lastKey = marshall({ id: 'last-id' });
      const result: QueryCommandOutput = {
        Items: items.map((item) => marshall(item)),
        LastEvaluatedKey: lastKey,
        $metadata: {},
      };

      const paged = toPagedResult(result);

      expect(paged.hasNextPage).toBe(true);
      expect(paged.nextPageKey).toBeDefined();
      expect(decodePageKey(paged.nextPageKey!)).toEqual(unmarshall(lastKey));
    });

    it('should handle empty results', () => {
      const result: QueryCommandOutput = {
        Items: [],
        LastEvaluatedKey: undefined,
        $metadata: {},
      };

      const paged = toPagedResult(result);

      expect(paged.items).toHaveLength(0);
      expect(paged.hasNextPage).toBe(false);
    });

    it('should work with scan results', () => {
      const items = [{ id: '1', name: 'Item 1' }];
      const result: ScanCommandOutput = {
        Items: items.map((item) => marshall(item)),
        LastEvaluatedKey: undefined,
        $metadata: {},
      };

      const paged = toPagedResult(result);

      expect(paged.items).toHaveLength(1);
      expect(paged.hasNextPage).toBe(false);
    });
  });

  describe('mapBatchWriteRequests', () => {
    it('should map put requests', () => {
      const requests = [
        {
          type: 'put' as const,
          item: { id: '1', name: 'Item 1' },
        },
      ];

      const result = mapBatchWriteRequests(requests);

      expect(result).toHaveLength(1);
      expect(result[0]).toHaveProperty('PutRequest');
      expect(result[0].PutRequest?.Item).toEqual({ id: '1', name: 'Item 1' });
    });

    it('should map delete requests', () => {
      const requests = [
        {
          type: 'delete' as const,
          key: { id: '1' },
        },
      ];

      const result = mapBatchWriteRequests(requests);

      expect(result).toHaveLength(1);
      expect(result[0]).toHaveProperty('DeleteRequest');
      expect(result[0].DeleteRequest?.Key).toEqual({ id: '1' });
    });

    it('should map mixed requests', () => {
      const requests = [
        {
          type: 'put' as const,
          item: { id: '1', name: 'Item 1' },
        },
        {
          type: 'delete' as const,
          key: { id: '2' },
        },
      ];

      const result = mapBatchWriteRequests(requests);

      expect(result).toHaveLength(2);
      expect(result[0]).toHaveProperty('PutRequest');
      expect(result[1]).toHaveProperty('DeleteRequest');
    });
  });

  describe('mapTransactItems', () => {
    it('should map put transaction items', () => {
      const requests = [
        {
          type: 'put' as const,
          item: { id: '1', name: 'Item 1' },
        },
      ];

      const result = mapTransactItems('test-table', requests);

      expect(result).toHaveLength(1);
      expect(result[0]).toHaveProperty('Put');
      expect(result[0].Put?.TableName).toBe('test-table');
      expect(result[0].Put?.Item).toEqual({ id: '1', name: 'Item 1' });
    });

    it('should map update transaction items', () => {
      const requests = [
        {
          type: 'update' as const,
          key: { id: '1' },
          updateExpression: 'SET #name = :name',
          expressionAttributeNames: { '#name': 'name' },
          expressionAttributeValues: { ':name': 'New Name' },
        },
      ];

      const result = mapTransactItems('test-table', requests);

      expect(result).toHaveLength(1);
      expect(result[0]).toHaveProperty('Update');
      expect(result[0].Update?.TableName).toBe('test-table');
      expect(result[0].Update?.Key).toEqual({ id: '1' });
      expect(result[0].Update?.UpdateExpression).toBe('SET #name = :name');
    });

    it('should map delete transaction items', () => {
      const requests = [
        {
          type: 'delete' as const,
          key: { id: '1' },
        },
      ];

      const result = mapTransactItems('test-table', requests);

      expect(result).toHaveLength(1);
      expect(result[0]).toHaveProperty('Delete');
      expect(result[0].Delete?.TableName).toBe('test-table');
      expect(result[0].Delete?.Key).toEqual({ id: '1' });
    });

    it('should include condition expressions', () => {
      const requests = [
        {
          type: 'put' as const,
          item: { id: '1', name: 'Item 1' },
          conditionExpression: 'attribute_not_exists(#id)',
        },
        {
          type: 'update' as const,
          key: { id: '2' },
          updateExpression: 'SET #name = :name',
          conditionExpression: 'attribute_exists(#id)',
        },
        {
          type: 'delete' as const,
          key: { id: '3' },
          conditionExpression: 'attribute_exists(#id)',
        },
      ];

      const result = mapTransactItems('test-table', requests);

      expect(result[0].Put?.ConditionExpression).toBe('attribute_not_exists(#id)');
      expect(result[1].Update?.ConditionExpression).toBe('attribute_exists(#id)');
      expect(result[2].Delete?.ConditionExpression).toBe('attribute_exists(#id)');
    });
  });

  describe('sortBatchResults', () => {
    it('should sort results to match input key order', () => {
      const keys = [{ id: '3' }, { id: '1' }, { id: '2' }];
      const items = [
        { id: '1', name: 'Item 1' },
        { id: '2', name: 'Item 2' },
        { id: '3', name: 'Item 3' },
      ];
      const keyConfig = { pk: 'id' };

      const result = sortBatchResults(keys, items, keyConfig);

      expect(result).toHaveLength(3);
      expect(result[0].id).toBe('3');
      expect(result[1].id).toBe('1');
      expect(result[2].id).toBe('2');
    });

    it('should handle composite keys', () => {
      const keys = [
        { id: '1', email: 'a@example.com' },
        { id: '1', email: 'b@example.com' },
      ];
      const items = [
        { id: '1', email: 'b@example.com', name: 'Item B' },
        { id: '1', email: 'a@example.com', name: 'Item A' },
      ];
      const keyConfig = { pk: 'id', sk: 'email' };

      const result = sortBatchResults(keys, items, keyConfig);

      expect(result).toHaveLength(2);
      expect(result[0].email).toBe('a@example.com');
      expect(result[1].email).toBe('b@example.com');
    });

    it('should filter out missing items', () => {
      const keys = [{ id: '1' }, { id: '2' }, { id: '3' }];
      const items = [{ id: '2', name: 'Item 2' }];
      const keyConfig = { pk: 'id' };

      const result = sortBatchResults(keys, items, keyConfig);

      expect(result).toHaveLength(1);
      expect(result[0].id).toBe('2');
    });
  });

  describe('sortKeyConditions', () => {
    it('should provide equals condition', () => {
      expect(sortKeyConditions.equals()).toBe('#sk = :sk');
    });

    it('should provide beginsWith condition', () => {
      expect(sortKeyConditions.beginsWith()).toBe('begins_with(#sk, :sk_prefix)');
    });

    it('should provide between condition', () => {
      expect(sortKeyConditions.between()).toBe('#sk BETWEEN :sk_start AND :sk_end');
    });

    it('should provide lessThan condition', () => {
      expect(sortKeyConditions.lessThan()).toBe('#sk < :sk');
    });

    it('should provide lessThanOrEqual condition', () => {
      expect(sortKeyConditions.lessThanOrEqual()).toBe('#sk <= :sk');
    });

    it('should provide greaterThan condition', () => {
      expect(sortKeyConditions.greaterThan()).toBe('#sk > :sk');
    });

    it('should provide greaterThanOrEqual condition', () => {
      expect(sortKeyConditions.greaterThanOrEqual()).toBe('#sk >= :sk');
    });
  });
});
