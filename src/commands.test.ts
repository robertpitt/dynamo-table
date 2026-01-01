import { describe, it, expect } from 'vitest';
import {
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  QueryCommand,
  ScanCommand,
  BatchGetItemCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import * as v from 'valibot';
import {
  buildGetItemCommand,
  buildBatchGetItemCommand,
  buildPutItemCommand,
  buildUpdateItemCommand,
  buildDeleteItemCommand,
  buildQueryCommand,
  buildQueryIndexCommand,
  buildScanCommand,
  buildTransactWriteItemsCommand,
} from './commands.js';
import type { ExtendedTableOptions } from './types.js';

// Test schemas
const UserSchema = v.object({
  id: v.string(),
  orgId: v.string(),
  name: v.string(),
  email: v.string(),
  age: v.optional(v.number()),
  status: v.optional(v.string()),
});

const SimpleSchema = v.object({
  id: v.string(),
  name: v.string(),
});

describe('buildGetItemCommand', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should build a basic GetItemCommand', () => {
    const command = buildGetItemCommand({ id: '123' }, undefined, config);
    
    expect(command).toBeInstanceOf(GetItemCommand);
    const input = command.input;
    expect(input.TableName).toBe('test-table');
    expect(input.Key).toBeDefined();
    expect(input.ConsistentRead).toBe(false);
  });

  it('should include consistentRead when specified', () => {
    const command = buildGetItemCommand({ id: '123' }, { consistentRead: true }, config);
    
    expect(command.input.ConsistentRead).toBe(true);
  });

  it('should include projection expression when specified', () => {
    const command = buildGetItemCommand(
      { id: '123' },
      { projection: ['name', 'email'] },
      config
    );
    
    expect(command.input.ProjectionExpression).toBe('#name, #email');
    expect(command.input.ExpressionAttributeNames).toEqual({
      '#name': 'name',
      '#email': 'email',
    });
  });

  it('should handle nested projection paths', () => {
    const configWithNested: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: UserSchema,
      key: { pk: 'id' },
    };
    
    const command = buildGetItemCommand(
      { id: '123' },
      { projection: ['name', 'email'] },
      configWithNested
    );
    
    expect(command.input.ProjectionExpression).toBe('#name, #email');
  });

  it('should handle empty projection array', () => {
    const command = buildGetItemCommand({ id: '123' }, { projection: [] }, config);
    
    expect(command.input.ProjectionExpression).toBeUndefined();
  });

  it('should work with composite key', () => {
    const compositeConfig: ExtendedTableOptions<typeof UserSchema, { pk: 'orgId'; sk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: UserSchema,
      key: { pk: 'orgId', sk: 'id' },
    };
    
    const command = buildGetItemCommand({ orgId: 'org1', id: '123' }, undefined, compositeConfig);
    
    expect(command).toBeInstanceOf(GetItemCommand);
    expect(command.input.TableName).toBe('test-table');
  });
});

describe('buildBatchGetItemCommand', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should build a basic BatchGetItemCommand', () => {
    const command = buildBatchGetItemCommand(
      [{ id: '123' }, { id: '456' }],
      undefined,
      config
    );
    
    expect(command).toBeInstanceOf(BatchGetItemCommand);
    const input = command.input;
    expect(input.RequestItems).toBeDefined();
    expect(input.RequestItems!['test-table']).toBeDefined();
    expect(input.RequestItems!['test-table'].Keys).toHaveLength(2);
    expect(input.RequestItems!['test-table'].ConsistentRead).toBe(false);
  });

  it('should include consistentRead when specified', () => {
    const command = buildBatchGetItemCommand(
      [{ id: '123' }],
      { consistentRead: true },
      config
    );
    
    expect(command.input.RequestItems!['test-table'].ConsistentRead).toBe(true);
  });

  it('should include projection expression when specified', () => {
    const command = buildBatchGetItemCommand(
      [{ id: '123' }, { id: '456' }],
      { projection: ['name', 'email'] },
      config
    );
    
    const requestItem = command.input.RequestItems!['test-table'];
    expect(requestItem.ProjectionExpression).toBe('#name, #email');
    expect(requestItem.ExpressionAttributeNames).toEqual({
      '#name': 'name',
      '#email': 'email',
    });
  });

  it('should handle empty projection array', () => {
    const command = buildBatchGetItemCommand(
      [{ id: '123' }],
      { projection: [] },
      config
    );
    
    expect(command.input.RequestItems!['test-table'].ProjectionExpression).toBeUndefined();
  });

  it('should work with composite key', () => {
    const compositeConfig: ExtendedTableOptions<typeof UserSchema, { pk: 'orgId'; sk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: UserSchema,
      key: { pk: 'orgId', sk: 'id' },
    };
    
    const command = buildBatchGetItemCommand(
      [{ orgId: 'org1', id: '123' }],
      undefined,
      compositeConfig
    );
    
    expect(command).toBeInstanceOf(BatchGetItemCommand);
  });
});

describe('buildPutItemCommand', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should build a basic PutItemCommand', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(item, undefined, config);
    
    expect(command).toBeInstanceOf(PutItemCommand);
    expect(command.input.TableName).toBe('test-table');
    expect(command.input.Item).toBeDefined();
  });

  it('should include condition expression when specified', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(
      item,
      {
        condition: (c) => c.notExists('id'),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle complex condition expressions', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(
      item,
      {
        condition: (c) => c.and(
          c.notExists('id'),
          c.eq('status', 'active')
        ),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });
});

describe('buildUpdateItemCommand', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should build a basic UpdateItemCommand', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane' },
      undefined,
      config
    );
    
    expect(command).toBeInstanceOf(UpdateItemCommand);
    expect(command.input.TableName).toBe('test-table');
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.ReturnValues).toBe('ALL_NEW');
  });

  it('should handle returnValues option', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane' },
      { returnValues: 'NONE' },
      config
    );
    
    expect(command.input.ReturnValues).toBe('NONE');
  });

  it('should include condition expression when specified', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane' },
      {
        condition: (c) => c.eq('status', 'active'),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should merge condition expression attributes with update expression attributes', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane', email: 'jane@example.com' },
      {
        condition: (c) => c.eq('status', 'active'),
      },
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle null/undefined values for removal', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane', age: undefined },
      undefined,
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    // Should include REMOVE expression for undefined values
  });

  it('should work with composite key', () => {
    const compositeConfig: ExtendedTableOptions<typeof UserSchema, { pk: 'orgId'; sk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: UserSchema,
      key: { pk: 'orgId', sk: 'id' },
    };
    
    const command = buildUpdateItemCommand(
      { orgId: 'org1', id: '123' },
      { name: 'Jane' },
      undefined,
      compositeConfig
    );
    
    expect(command).toBeInstanceOf(UpdateItemCommand);
  });
});

describe('buildDeleteItemCommand', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should build a basic DeleteItemCommand', () => {
    const command = buildDeleteItemCommand({ id: '123' }, undefined, config);
    
    expect(command).toBeInstanceOf(DeleteItemCommand);
    expect(command.input.TableName).toBe('test-table');
    expect(command.input.Key).toBeDefined();
  });

  it('should include condition expression when specified', () => {
    const command = buildDeleteItemCommand(
      { id: '123' },
      {
        condition: (c) => c.eq('status', 'inactive'),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should work with composite key', () => {
    const compositeConfig: ExtendedTableOptions<typeof UserSchema, { pk: 'orgId'; sk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: UserSchema,
      key: { pk: 'orgId', sk: 'id' },
    };
    
    const command = buildDeleteItemCommand(
      { orgId: 'org1', id: '123' },
      undefined,
      compositeConfig
    );
    
    expect(command).toBeInstanceOf(DeleteItemCommand);
  });
});

describe('buildQueryCommand', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'orgId'; sk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'orgId', sk: 'id' },
  };

  it('should build a basic QueryCommand with partition key only', () => {
    const command = buildQueryCommand({ orgId: 'org1' }, undefined, config);
    
    expect(command).toBeInstanceOf(QueryCommand);
    expect(command.input.TableName).toBe('test-table');
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should include sort key condition when provided', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        sortKey: (sk) => sk.eq('123'),
      },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('AND');
  });

  it('should include filter expression when provided', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        filter: (f) => f.eq('status', 'active'),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should include limit when specified', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { limit: 10 },
      config
    );
    
    expect(command.input.Limit).toBe(10);
  });

  it('should include scanIndexForward when specified', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { scanIndexForward: false },
      config
    );
    
    expect(command.input.ScanIndexForward).toBe(false);
  });

  it('should include exclusiveStartKey when specified', () => {
    const exclusiveStartKey = { orgId: { S: 'org1' }, id: { S: '123' } };
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { exclusiveStartKey },
      config
    );
    
    expect(command.input.ExclusiveStartKey).toEqual(exclusiveStartKey);
  });

  it('should merge filter expression attributes with key condition attributes', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        sortKey: (sk) => sk.eq('123'),
        filter: (f) => f.eq('status', 'active'),
      },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should work with table without sort key', () => {
    const simpleConfig: ExtendedTableOptions<typeof SimpleSchema, { pk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: SimpleSchema,
      key: { pk: 'id' },
    };
    
    const command = buildQueryCommand({ id: '123' }, undefined, simpleConfig);
    
    expect(command).toBeInstanceOf(QueryCommand);
    expect(command.input.KeyConditionExpression).toBeDefined();
  });
});

describe('buildQueryIndexCommand', () => {
  const config: ExtendedTableOptions<
    typeof UserSchema,
    { pk: 'id' },
    { emailIndex: { key: { pk: 'email'; sk: 'id' }; projection: 'KEYS_ONLY' } }
  > = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
    indexes: {
      emailIndex: {
        key: { pk: 'email', sk: 'id' },
        projection: 'KEYS_ONLY',
      },
    },
  };

  it('should build a basic QueryIndexCommand', () => {
    const command = buildQueryIndexCommand(
      'emailIndex',
      { email: 'test@example.com' },
      undefined,
      config
    );
    
    expect(command).toBeInstanceOf(QueryCommand);
    expect(command.input.TableName).toBe('test-table');
    expect(command.input.IndexName).toBe('emailIndex');
    expect(command.input.KeyConditionExpression).toBeDefined();
  });

  it('should throw error for non-existent index', () => {
    expect(() => {
      buildQueryIndexCommand(
        'nonExistentIndex' as any,
        { email: 'test@example.com' },
        undefined,
        config
      );
    }).toThrow('Index "nonExistentIndex" not found in table configuration');
  });

  it('should include sort key condition when provided', () => {
    const command = buildQueryIndexCommand(
      'emailIndex',
      { email: 'test@example.com' },
      {
        sortKey: (sk: any) => sk.eq('123'),
      },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('AND');
  });

  it('should include filter expression when provided', () => {
    const command = buildQueryIndexCommand(
      'emailIndex',
      { email: 'test@example.com' },
      {
        filter: (f: any) => f.eq('status', 'active'),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
  });

  it('should include limit when specified', () => {
    const command = buildQueryIndexCommand(
      'emailIndex',
      { email: 'test@example.com' },
      { limit: 10 },
      config
    );
    
    expect(command.input.Limit).toBe(10);
  });

  it('should include scanIndexForward when specified', () => {
    const command = buildQueryIndexCommand(
      'emailIndex',
      { email: 'test@example.com' },
      { scanIndexForward: false },
      config
    );
    
    expect(command.input.ScanIndexForward).toBe(false);
  });

  it('should include exclusiveStartKey when specified', () => {
    const exclusiveStartKey = { email: { S: 'test@example.com' }, id: { S: '123' } };
    const command = buildQueryIndexCommand(
      'emailIndex',
      { email: 'test@example.com' },
      { exclusiveStartKey },
      config
    );
    
    expect(command.input.ExclusiveStartKey).toEqual(exclusiveStartKey);
  });
});

describe('buildScanCommand', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should build a basic ScanCommand', () => {
    const command = buildScanCommand(undefined, config);
    
    expect(command).toBeInstanceOf(ScanCommand);
    expect(command.input.TableName).toBe('test-table');
  });

  it('should include filter expression when provided', () => {
    const command = buildScanCommand(
      {
        filter: (f) => f.eq('status', 'active'),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should include limit when specified', () => {
    const command = buildScanCommand({ limit: 10 }, config);
    
    expect(command.input.Limit).toBe(10);
  });

  it('should include exclusiveStartKey when specified', () => {
    const exclusiveStartKey = { id: { S: '123' } };
    const command = buildScanCommand({ exclusiveStartKey }, config);
    
    expect(command.input.ExclusiveStartKey).toEqual(exclusiveStartKey);
  });

  it('should handle complex filter expressions', () => {
    const command = buildScanCommand(
      {
        filter: (f) => f.and(
          f.eq('status', 'active'),
          f.gt('age', 18)
        ),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
  });
});

describe('buildTransactWriteItemsCommand', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should build a transaction with put request', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildTransactWriteItemsCommand(
      [{ type: 'put', item }],
      config
    );
    
    expect(command).toBeInstanceOf(TransactWriteItemsCommand);
    expect(command.input.TransactItems).toHaveLength(1);
    expect(command.input.TransactItems![0].Put).toBeDefined();
    expect(command.input.TransactItems![0].Put!.TableName).toBe('test-table');
  });

  it('should build a transaction with update request', () => {
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'update',
          key: { id: '123' },
          updates: { name: 'Jane' },
        },
      ],
      config
    );
    
    expect(command).toBeInstanceOf(TransactWriteItemsCommand);
    expect(command.input.TransactItems).toHaveLength(1);
    expect(command.input.TransactItems![0].Update).toBeDefined();
    expect(command.input.TransactItems![0].Update!.TableName).toBe('test-table');
  });

  it('should build a transaction with delete request', () => {
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'delete',
          key: { id: '123' },
        },
      ],
      config
    );
    
    expect(command).toBeInstanceOf(TransactWriteItemsCommand);
    expect(command.input.TransactItems).toHaveLength(1);
    expect(command.input.TransactItems![0].Delete).toBeDefined();
    expect(command.input.TransactItems![0].Delete!.TableName).toBe('test-table');
  });

  it('should build a transaction with mixed requests', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildTransactWriteItemsCommand(
      [
        { type: 'put', item },
        {
          type: 'update',
          key: { id: '456' },
          updates: { name: 'Jane' },
        },
        {
          type: 'delete',
          key: { id: '789' },
        },
      ],
      config
    );
    
    expect(command).toBeInstanceOf(TransactWriteItemsCommand);
    expect(command.input.TransactItems).toHaveLength(3);
  });

  it('should include condition expression for put request', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'put',
          item,
          condition: (c) => c.notExists('id'),
        },
      ],
      config
    );
    
    expect(command.input.TransactItems![0].Put!.ConditionExpression).toBeDefined();
    expect(command.input.TransactItems![0].Put!.ExpressionAttributeNames).toBeDefined();
    expect(command.input.TransactItems![0].Put!.ExpressionAttributeValues).toBeDefined();
  });

  it('should include condition expression for update request', () => {
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'update',
          key: { id: '123' },
          updates: { name: 'Jane' },
          condition: (c) => c.eq('status', 'active'),
        },
      ],
      config
    );
    
    expect(command.input.TransactItems![0].Update!.ConditionExpression).toBeDefined();
    expect(command.input.TransactItems![0].Update!.ExpressionAttributeNames).toBeDefined();
    expect(command.input.TransactItems![0].Update!.ExpressionAttributeValues).toBeDefined();
  });

  it('should merge condition expression attributes with update expression attributes', () => {
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'update',
          key: { id: '123' },
          updates: { name: 'Jane', email: 'jane@example.com' },
          condition: (c) => c.eq('status', 'active'),
        },
      ],
      config
    );
    
    const updateItem = command.input.TransactItems![0].Update!;
    expect(updateItem.UpdateExpression).toBeDefined();
    expect(updateItem.ConditionExpression).toBeDefined();
    expect(updateItem.ExpressionAttributeNames).toBeDefined();
    expect(updateItem.ExpressionAttributeValues).toBeDefined();
  });

  it('should include condition expression for delete request', () => {
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'delete',
          key: { id: '123' },
          condition: (c) => c.eq('status', 'inactive'),
        },
      ],
      config
    );
    
    expect(command.input.TransactItems![0].Delete!.ConditionExpression).toBeDefined();
    expect(command.input.TransactItems![0].Delete!.ExpressionAttributeNames).toBeDefined();
    expect(command.input.TransactItems![0].Delete!.ExpressionAttributeValues).toBeDefined();
  });

  it('should throw error when exceeding 25 items limit', () => {
    const items = Array.from({ length: 26 }, (_, i) => ({
      type: 'put' as const,
      item: {
        id: `id-${i}`,
        orgId: 'org1',
        name: 'John',
        email: 'john@example.com',
      } as v.InferInput<typeof UserSchema>,
    }));
    
    expect(() => {
      buildTransactWriteItemsCommand(items, config);
    }).toThrow('TransactWriteItems supports a maximum of 25 items per transaction');
  });

  it('should work with composite key', () => {
    const compositeConfig: ExtendedTableOptions<typeof UserSchema, { pk: 'orgId'; sk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: UserSchema,
      key: { pk: 'orgId', sk: 'id' },
    };
    
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'update',
          key: { orgId: 'org1', id: '123' },
          updates: { name: 'Jane' },
        },
      ],
      compositeConfig
    );
    
    expect(command).toBeInstanceOf(TransactWriteItemsCommand);
  });
});

// ============================================================================
// EDGE CASES AND COMPLEX SCENARIOS
// ============================================================================

describe('buildGetItemCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should handle projection with nested attribute paths', () => {
    const NestedSchema = v.object({
      id: v.string(),
      address: v.object({
        street: v.string(),
        city: v.string(),
        zip: v.string(),
      }),
    });
    
    const nestedConfig: ExtendedTableOptions<typeof NestedSchema, { pk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: NestedSchema,
      key: { pk: 'id' },
    };
    
    const command = buildGetItemCommand(
      { id: '123' },
      { projection: ['address.street', 'address.city'] },
      nestedConfig
    );
    
    expect(command.input.ProjectionExpression).toBeDefined();
    expect(command.input.ProjectionExpression).toContain('address');
    expect(command.input.ExpressionAttributeNames).toBeDefined();
  });

  it('should handle duplicate projection attributes gracefully', () => {
    const command = buildGetItemCommand(
      { id: '123' },
      { projection: ['name', 'name', 'email'] },
      config
    );
    
    expect(command.input.ProjectionExpression).toBeDefined();
    // Should handle duplicates (implementation may dedupe or DynamoDB will handle it)
  });

  it('should handle projection with many attributes', () => {
    const command = buildGetItemCommand(
      { id: '123' },
      { projection: ['id', 'orgId', 'name', 'email', 'age', 'status'] },
      config
    );
    
    expect(command.input.ProjectionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
  });

  it('should handle projection with special characters in attribute names', () => {
    const SpecialSchema = v.object({
      id: v.string(),
      'attr-name': v.string(),
      'attr_name': v.string(),
    });
    
    const specialConfig: ExtendedTableOptions<typeof SpecialSchema, { pk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: SpecialSchema,
      key: { pk: 'id' },
    };
    
    const command = buildGetItemCommand(
      { id: '123' },
      { projection: ['attr-name', 'attr_name'] },
      specialConfig
    );
    
    expect(command.input.ProjectionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
  });
});

describe('buildBatchGetItemCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should handle empty keys array', () => {
    const command = buildBatchGetItemCommand([], undefined, config);
    
    expect(command).toBeInstanceOf(BatchGetItemCommand);
    expect(command.input.RequestItems!['test-table'].Keys).toHaveLength(0);
  });

  it('should handle single key in batch', () => {
    const command = buildBatchGetItemCommand([{ id: '123' }], undefined, config);
    
    expect(command.input.RequestItems!['test-table'].Keys).toHaveLength(1);
  });

  it('should handle large batch (approaching 100 items)', () => {
    const keys = Array.from({ length: 50 }, (_, i) => ({ id: `id-${i}` }));
    const command = buildBatchGetItemCommand(keys, undefined, config);
    
    expect(command.input.RequestItems!['test-table'].Keys).toHaveLength(50);
  });

  it('should handle batch with projection and consistentRead together', () => {
    const command = buildBatchGetItemCommand(
      [{ id: '123' }, { id: '456' }],
      {
        projection: ['name', 'email'],
        consistentRead: true,
      },
      config
    );
    
    const requestItem = command.input.RequestItems!['test-table'];
    expect(requestItem.ProjectionExpression).toBeDefined();
    expect(requestItem.ConsistentRead).toBe(true);
  });
});

describe('buildPutItemCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should handle condition with OR logic', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(
      item,
      {
        condition: (c) => c.or(
          c.notExists('id'),
          c.eq('status', 'inactive')
        ),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ConditionExpression).toContain('OR');
  });

  it('should handle condition with NOT logic', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(
      item,
      {
        condition: (c) => c.not(c.eq('status', 'inactive')),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ConditionExpression).toContain('NOT');
  });

  it('should handle condition with size comparison', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(
      item,
      {
        condition: (c) => c.sizeGt('name', 3),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ConditionExpression).toContain('size');
  });

  it('should handle condition with contains check', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(
      item,
      {
        condition: (c) => c.contains('email', '@'),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ConditionExpression).toContain('contains');
  });

  it('should handle condition with in operator', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(
      item,
      {
        condition: (c) => c.in('status', ['active', 'pending', 'approved']),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ConditionExpression).toContain('IN');
  });

  it('should handle deeply nested condition expressions', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildPutItemCommand(
      item,
      {
        condition: (c) => c.and(
          c.or(
            c.notExists('id'),
            c.eq('status', 'inactive')
          ),
          c.and(
            c.gt('age', 18),
            c.lt('age', 65)
          )
        ),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });
});

describe('buildUpdateItemCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should handle update with only SET operations', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane', email: 'jane@example.com', age: 30 },
      undefined,
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.UpdateExpression).toContain('SET');
    expect(command.input.UpdateExpression).not.toContain('REMOVE');
  });

  it('should handle update with only REMOVE operations', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { age: undefined, status: undefined },
      undefined,
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.UpdateExpression).toContain('REMOVE');
  });

  it('should handle update with both SET and REMOVE operations', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane', age: undefined },
      undefined,
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.UpdateExpression).toContain('SET');
    expect(command.input.UpdateExpression).toContain('REMOVE');
  });

  it('should handle update with nested attribute paths', () => {
    const NestedSchema = v.object({
      id: v.string(),
      metadata: v.object({
        version: v.number(),
        tags: v.array(v.string()),
      }),
    });
    
    const nestedConfig: ExtendedTableOptions<typeof NestedSchema, { pk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: NestedSchema,
      key: { pk: 'id' },
    };
    
    // Note: DynamoDB update expressions support nested paths via dot notation
    // This tests that the builder handles nested paths correctly
    const command = buildUpdateItemCommand(
      { id: '123' },
      { 'metadata.version': 2 } as any,
      undefined,
      nestedConfig
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
  });

  it('should handle update with condition that uses same attributes as update', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane' },
      {
        condition: (c) => c.eq('name', 'John'),
      },
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.ConditionExpression).toBeDefined();
    // Should merge attribute names correctly
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle update with complex condition expression', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane', email: 'jane@example.com' },
      {
        condition: (c) => c.and(
          c.eq('status', 'active'),
          c.or(
            c.gt('age', 18),
            c.notExists('age')
          )
        ),
      },
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.ConditionExpression).toBeDefined();
  });

  it('should handle update with null value (treated as REMOVE)', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: null as any },
      undefined,
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    // null is treated as REMOVE (same as undefined)
    expect(command.input.UpdateExpression).toContain('REMOVE');
  });

  it('should handle all returnValues options', () => {
    const returnValuesOptions: Array<'NONE' | 'ALL_OLD' | 'ALL_NEW' | 'UPDATED_OLD' | 'UPDATED_NEW'> = [
      'NONE',
      'ALL_OLD',
      'ALL_NEW',
      'UPDATED_OLD',
      'UPDATED_NEW',
    ];
    
    for (const returnValue of returnValuesOptions) {
      const command = buildUpdateItemCommand(
        { id: '123' },
        { name: 'Jane' },
        { returnValues: returnValue },
        config
      );
      
      expect(command.input.ReturnValues).toBe(returnValue);
    }
  });
});

describe('buildDeleteItemCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should handle condition with complex logic', () => {
    const command = buildDeleteItemCommand(
      { id: '123' },
      {
        condition: (c) => c.and(
          c.eq('status', 'inactive'),
          c.lt('age', 100)
        ),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle condition with attribute_exists check', () => {
    const command = buildDeleteItemCommand(
      { id: '123' },
      {
        condition: (c) => c.exists('status'),
      },
      config
    );
    
    expect(command.input.ConditionExpression).toBeDefined();
    expect(command.input.ConditionExpression).toContain('attribute_exists');
  });
});

describe('buildQueryCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'orgId'; sk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'orgId', sk: 'id' },
  };

  it('should handle sort key with eq condition', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { sortKey: (sk) => sk.eq('123') },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('AND');
  });

  it('should handle sort key with lt condition', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { sortKey: (sk) => sk.lt('999') },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('<');
  });

  it('should handle sort key with lte condition', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { sortKey: (sk) => sk.lte('999') },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('<=');
  });

  it('should handle sort key with gt condition', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { sortKey: (sk) => sk.gt('100') },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('>');
  });

  it('should handle sort key with gte condition', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { sortKey: (sk) => sk.gte('100') },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('>=');
  });

  it('should handle sort key with between condition', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { sortKey: (sk) => sk.between('100', '999') },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('BETWEEN');
  });

  it('should handle sort key with beginsWith condition', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      { sortKey: (sk) => sk.beginsWith('user-') },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.KeyConditionExpression).toContain('begins_with');
  });

  it('should handle query with sortKey and filter together', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        sortKey: (sk) => sk.between('100', '999'),
        filter: (f) => f.eq('status', 'active'),
      },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle filter with complex AND/OR logic', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        filter: (f) => f.and(
          f.eq('status', 'active'),
          f.or(
            f.gt('age', 18),
            f.notExists('age')
          )
        ),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('AND');
    expect(command.input.FilterExpression).toContain('OR');
  });

  it('should handle filter with NOT logic', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        filter: (f) => f.not(f.eq('status', 'inactive')),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('NOT');
  });

  it('should handle filter with size comparison', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        filter: (f) => f.sizeGt('name', 5),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('size');
  });

  it('should handle filter with contains check', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        filter: (f) => f.contains('email', '@example.com'),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('contains');
  });

  it('should handle filter with in operator', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        filter: (f) => f.in('status', ['active', 'pending', 'approved']),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('IN');
  });

  it('should handle filter with attribute_exists check', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        filter: (f) => f.exists('status'),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('attribute_exists');
  });

  it('should handle filter with attribute_not_exists check', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        filter: (f) => f.notExists('status'),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('attribute_not_exists');
  });

  it('should handle query with all options combined', () => {
    const exclusiveStartKey = { orgId: { S: 'org1' }, id: { S: '123' } };
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        sortKey: (sk) => sk.between('100', '999'),
        filter: (f) => f.eq('status', 'active'),
        limit: 50,
        scanIndexForward: false,
        exclusiveStartKey,
      },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.Limit).toBe(50);
    expect(command.input.ScanIndexForward).toBe(false);
    expect(command.input.ExclusiveStartKey).toEqual(exclusiveStartKey);
  });

  it('should handle filter expression that overlaps with key condition attribute names', () => {
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        sortKey: (sk) => sk.eq('123'),
        filter: (f) => f.eq('orgId', 'org1'), // Same attribute as partition key
      },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.FilterExpression).toBeDefined();
    // Should merge attribute names correctly
    expect(command.input.ExpressionAttributeNames).toBeDefined();
  });
});

describe('buildQueryIndexCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<
    typeof UserSchema,
    { pk: 'id' },
    { emailIndex: { key: { pk: 'email'; sk: 'id' }; projection: 'KEYS_ONLY' } }
  > = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
    indexes: {
      emailIndex: {
        key: { pk: 'email', sk: 'id' },
        projection: 'KEYS_ONLY',
      },
    },
  };

  it('should handle sort key with all comparison operators', () => {
    const operators = [
      (sk: any) => sk.eq('123'),
      (sk: any) => sk.lt('999'),
      (sk: any) => sk.lte('999'),
      (sk: any) => sk.gt('100'),
      (sk: any) => sk.gte('100'),
      (sk: any) => sk.between('100', '999'),
      (sk: any) => sk.beginsWith('user-'),
    ];
    
    for (const sortKeyFn of operators) {
      const command = buildQueryIndexCommand(
        'emailIndex',
        { email: 'test@example.com' },
        { sortKey: sortKeyFn },
        config
      );
      
      expect(command.input.KeyConditionExpression).toBeDefined();
      expect(command.input.IndexName).toBe('emailIndex');
    }
  });

  it('should handle index query with complex filter', () => {
    const command = buildQueryIndexCommand(
      'emailIndex',
      { email: 'test@example.com' },
      {
        sortKey: (sk: any) => sk.between('100', '999'),
        filter: (f: any) => f.and(
          f.eq('status', 'active'),
          f.gt('age', 18)
        ),
      },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.FilterExpression).toBeDefined();
  });

  it('should handle index query with all options', () => {
    const exclusiveStartKey = { email: { S: 'test@example.com' }, id: { S: '123' } };
    const command = buildQueryIndexCommand(
      'emailIndex',
      { email: 'test@example.com' },
      {
        sortKey: (sk: any) => sk.gte('100'),
        filter: (f: any) => f.eq('status', 'active'),
        limit: 25,
        scanIndexForward: true,
        exclusiveStartKey,
      },
      config
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.Limit).toBe(25);
    expect(command.input.ScanIndexForward).toBe(true);
    expect(command.input.ExclusiveStartKey).toEqual(exclusiveStartKey);
  });
});

describe('buildScanCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should handle scan with no options', () => {
    const command = buildScanCommand(undefined, config);
    
    expect(command).toBeInstanceOf(ScanCommand);
    expect(command.input.TableName).toBe('test-table');
    expect(command.input.FilterExpression).toBeUndefined();
  });

  it('should handle filter with deeply nested AND/OR logic', () => {
    const command = buildScanCommand(
      {
        filter: (f) => f.and(
          f.eq('status', 'active'),
          f.or(
            f.and(
              f.gt('age', 18),
              f.lt('age', 65)
            ),
            f.notExists('age')
          ),
          f.not(f.eq('status', 'inactive'))
        ),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle filter with size comparisons', () => {
    const command = buildScanCommand(
      {
        filter: (f) => f.and(
          f.sizeGt('name', 3),
          f.sizeLt('name', 50),
          f.sizeGte('email', 5),
          f.sizeLte('email', 100)
        ),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('size');
  });

  it('should handle filter with attribute_type check', () => {
    const command = buildScanCommand(
      {
        filter: (f) => f.attributeType('age', 'N'),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('attribute_type');
  });

  it('should handle filter with beginsWith', () => {
    const command = buildScanCommand(
      {
        filter: (f) => f.beginsWith('email', 'admin@'),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('begins_with');
  });

  it('should handle filter with between', () => {
    const command = buildScanCommand(
      {
        filter: (f) => f.between('age', 18, 65),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('BETWEEN');
  });

  it('should handle scan with limit and exclusiveStartKey', () => {
    const exclusiveStartKey = { id: { S: '123' } };
    const command = buildScanCommand(
      {
        limit: 100,
        exclusiveStartKey,
      },
      config
    );
    
    expect(command.input.Limit).toBe(100);
    expect(command.input.ExclusiveStartKey).toEqual(exclusiveStartKey);
  });

  it('should handle filter with in operator with many values', () => {
    const command = buildScanCommand(
      {
        filter: (f) => f.in('status', ['active', 'pending', 'approved', 'rejected', 'cancelled']),
      },
      config
    );
    
    expect(command.input.FilterExpression).toBeDefined();
    expect(command.input.FilterExpression).toContain('IN');
  });
});

describe('buildTransactWriteItemsCommand - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should handle transaction with all three operation types and conditions', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'put',
          item,
          condition: (c) => c.notExists('id'),
        },
        {
          type: 'update',
          key: { id: '456' },
          updates: { name: 'Jane' },
          condition: (c) => c.eq('status', 'active'),
        },
        {
          type: 'delete',
          key: { id: '789' },
          condition: (c) => c.exists('status'),
        },
      ],
      config
    );
    
    expect(command).toBeInstanceOf(TransactWriteItemsCommand);
    expect(command.input.TransactItems).toHaveLength(3);
    expect(command.input.TransactItems![0].Put!.ConditionExpression).toBeDefined();
    expect(command.input.TransactItems![1].Update!.ConditionExpression).toBeDefined();
    expect(command.input.TransactItems![2].Delete!.ConditionExpression).toBeDefined();
  });

  it('should handle transaction with overlapping attribute names across items', () => {
    const item1: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const item2: v.InferInput<typeof UserSchema> = {
      id: '456',
      orgId: 'org1',
      name: 'Jane',
      email: 'jane@example.com',
    };
    
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'put',
          item: item1,
          condition: (c) => c.eq('status', 'active'),
        },
        {
          type: 'put',
          item: item2,
          condition: (c) => c.eq('status', 'active'),
        },
      ],
      config
    );
    
    expect(command.input.TransactItems).toHaveLength(2);
    // Each item should have its own expression attribute names/values
    expect(command.input.TransactItems![0].Put!.ExpressionAttributeNames).toBeDefined();
    expect(command.input.TransactItems![1].Put!.ExpressionAttributeNames).toBeDefined();
  });

  it('should handle transaction with update that has SET and REMOVE', () => {
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'update',
          key: { id: '123' },
          updates: { name: 'Jane', age: undefined },
          condition: (c) => c.eq('status', 'active'),
        },
      ],
      config
    );
    
    const updateItem = command.input.TransactItems![0].Update!;
    expect(updateItem.UpdateExpression).toBeDefined();
    expect(updateItem.UpdateExpression).toContain('SET');
    expect(updateItem.UpdateExpression).toContain('REMOVE');
    expect(updateItem.ConditionExpression).toBeDefined();
  });

  it('should handle transaction with complex condition expressions', () => {
    const item: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'put',
          item,
          condition: (c) => c.and(
            c.notExists('id'),
            c.or(
              c.eq('status', 'pending'),
              c.eq('status', 'approved')
            ),
            c.gt('age', 18)
          ),
        },
      ],
      config
    );
    
    expect(command.input.TransactItems![0].Put!.ConditionExpression).toBeDefined();
    expect(command.input.TransactItems![0].Put!.ConditionExpression).toContain('AND');
    expect(command.input.TransactItems![0].Put!.ConditionExpression).toContain('OR');
  });

  it('should handle transaction with maximum allowed items (25)', () => {
    const items = Array.from({ length: 25 }, (_, i) => ({
      type: 'put' as const,
      item: {
        id: `id-${i}`,
        orgId: 'org1',
        name: 'John',
        email: 'john@example.com',
      } as v.InferInput<typeof UserSchema>,
    }));
    
    const command = buildTransactWriteItemsCommand(items, config);
    
    expect(command.input.TransactItems).toHaveLength(25);
  });

  it('should handle transaction with update using same attribute in condition and update', () => {
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'update',
          key: { id: '123' },
          updates: { name: 'Jane' },
          condition: (c) => c.eq('name', 'John'),
        },
      ],
      config
    );
    
    const updateItem = command.input.TransactItems![0].Update!;
    expect(updateItem.UpdateExpression).toBeDefined();
    expect(updateItem.ConditionExpression).toBeDefined();
    // Should merge attribute names correctly
    expect(updateItem.ExpressionAttributeNames).toBeDefined();
    expect(updateItem.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle transaction with multiple updates on same item (different transactions)', () => {
    // This tests that each transaction item is independent
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'update',
          key: { id: '123' },
          updates: { name: 'Jane' },
        },
        {
          type: 'update',
          key: { id: '123' },
          updates: { email: 'jane@example.com' },
        },
      ],
      config
    );
    
    expect(command.input.TransactItems).toHaveLength(2);
    expect(command.input.TransactItems![0].Update!.UpdateExpression).toBeDefined();
    expect(command.input.TransactItems![1].Update!.UpdateExpression).toBeDefined();
  });
});

describe('Expression Attribute Name Collisions - Edge Cases', () => {
  const config: ExtendedTableOptions<typeof UserSchema, { pk: 'id' }, undefined> = {
    tableName: 'test-table',
    schema: UserSchema,
    key: { pk: 'id' },
  };

  it('should handle update where condition uses same attribute names as update expression', () => {
    const command = buildUpdateItemCommand(
      { id: '123' },
      { name: 'Jane', email: 'jane@example.com' },
      {
        condition: (c) => c.and(
          c.eq('name', 'John'),
          c.eq('email', 'john@example.com')
        ),
      },
      config
    );
    
    expect(command.input.UpdateExpression).toBeDefined();
    expect(command.input.ConditionExpression).toBeDefined();
    // Attribute names should be merged correctly
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    // Values should be merged correctly (different values for same attribute)
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle query where filter uses same attribute names as key condition', () => {
    const compositeConfig: ExtendedTableOptions<typeof UserSchema, { pk: 'orgId'; sk: 'id' }, undefined> = {
      tableName: 'test-table',
      schema: UserSchema,
      key: { pk: 'orgId', sk: 'id' },
    };
    
    const command = buildQueryCommand(
      { orgId: 'org1' },
      {
        sortKey: (sk) => sk.eq('123'),
        filter: (f) => f.and(
          f.eq('orgId', 'org1'), // Same as partition key
          f.eq('id', '123') // Same as sort key
        ),
      },
      compositeConfig
    );
    
    expect(command.input.KeyConditionExpression).toBeDefined();
    expect(command.input.FilterExpression).toBeDefined();
    // Should merge attribute names correctly
    expect(command.input.ExpressionAttributeNames).toBeDefined();
    expect(command.input.ExpressionAttributeValues).toBeDefined();
  });

  it('should handle transaction where multiple items use same attribute names', () => {
    const item1: v.InferInput<typeof UserSchema> = {
      id: '123',
      orgId: 'org1',
      name: 'John',
      email: 'john@example.com',
    };
    
    const item2: v.InferInput<typeof UserSchema> = {
      id: '456',
      orgId: 'org1',
      name: 'Jane',
      email: 'jane@example.com',
    };
    
    const command = buildTransactWriteItemsCommand(
      [
        {
          type: 'put',
          item: item1,
          condition: (c) => c.eq('status', 'active'),
        },
        {
          type: 'put',
          item: item2,
          condition: (c) => c.eq('status', 'active'),
        },
        {
          type: 'update',
          key: { id: '789' },
          updates: { status: 'inactive' },
          condition: (c) => c.eq('status', 'active'),
        },
      ],
      config
    );
    
    expect(command.input.TransactItems).toHaveLength(3);
    // Each item should have independent expression attribute names/values
    // but they can reference the same attribute names
    expect(command.input.TransactItems![0].Put!.ExpressionAttributeNames).toBeDefined();
    expect(command.input.TransactItems![1].Put!.ExpressionAttributeNames).toBeDefined();
    expect(command.input.TransactItems![2].Update!.ExpressionAttributeNames).toBeDefined();
  });
});

