import { describe, it, expect, vi, beforeEach } from 'vitest';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { table } from '../../src/table.js';
import { createMockDynamoDBClient, createTestEntity, type TestEntity } from './test-helpers.js';
import * as v from 'valibot';

// Create a simple schema for testing
const TestSchema = v.object({
  id: v.string(),
  name: v.string(),
  email: v.string(),
  createdAt: v.string(),
});

describe('table', () => {
  let mockClient: DynamoDBClient;
  let mockSend: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    const mock = createMockDynamoDBClient();
    mockClient = mock.client;
    mockSend = mock.mockSend;
  });

  describe('table factory', () => {
    it('should create a table with entity methods', () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' } as const,
            schema: TestSchema,
          },
        },
      });

      expect(testTable.User).toBeDefined();
      expect(testTable.User.get).toBeInstanceOf(Function);
      expect(testTable.User.put).toBeInstanceOf(Function);
      expect(testTable.User.update).toBeInstanceOf(Function);
      expect(testTable.User.delete).toBeInstanceOf(Function);
      expect(testTable.User.query).toBeInstanceOf(Function);
      expect(testTable.User.scan).toBeInstanceOf(Function);
      expect(testTable.User.paginate).toBeInstanceOf(Function);
      expect(testTable.User.transaction).toBeInstanceOf(Function);
    });

    it('should support multiple entities', () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
          Order: {
            key: { pk: 'id', sk: 'orderId' },
            schema: v.object({
              id: v.string(),
              orderId: v.string(),
              total: v.number(),
            }),
          },
        },
      });

      expect(testTable.User).toBeDefined();
      expect(testTable.Order).toBeDefined();
    });
  });

  describe('get', () => {
    it('should get an item by key', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const testItem = createTestEntity();
      mockSend.mockResolvedValueOnce({
        Item: marshall(testItem),
      });

      const result = await testTable.User.get({ id: 'test-id' });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TableName: 'test-table',
            Key: marshall({ id: 'test-id' }),
          }),
        })
      );
      expect(result.item).toEqual(testItem);
    });

    it('should return null when item not found', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Item: undefined,
      });

      const result = await testTable.User.get({ id: 'non-existent' });

      expect(result.item).toBeNull();
    });

    it('should support composite keys', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id', sk: 'email' },
            schema: TestSchema,
          },
        },
      });

      const testItem = createTestEntity();
      mockSend.mockResolvedValueOnce({
        Item: marshall(testItem),
      });

      await testTable.User.get({ id: 'test-id', email: 'test@example.com' });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            Key: marshall({ id: 'test-id', email: 'test@example.com' }),
          }),
        })
      );
    });

    it('should pass through GetOptions', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Item: marshall(createTestEntity()),
      });

      await testTable.User.get(
        { id: 'test-id' },
        {
          ConsistentRead: true,
          ProjectionExpression: '#name, #email',
          ExpressionAttributeNames: { '#name': 'name', '#email': 'email' },
        }
      );

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            ConsistentRead: true,
            ProjectionExpression: '#name, #email',
            ExpressionAttributeNames: { '#name': 'name', '#email': 'email' },
          }),
        })
      );
    });
  });

  describe('put', () => {
    it('should put an item', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const testItem = createTestEntity();
      mockSend.mockResolvedValueOnce({});

      const result = await testTable.User.put(testItem);

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TableName: 'test-table',
            Item: marshall(testItem),
          }),
        })
      );
      expect(result.item).toEqual(testItem);
    });

    it('should return returned attributes when ReturnValues is specified', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const testItem = createTestEntity();
      const returnedItem = { ...testItem, updatedAt: '2024-01-02T00:00:00Z' };
      mockSend.mockResolvedValueOnce({
        Attributes: marshall(returnedItem),
      });

      const result = await testTable.User.put(testItem, {
        ReturnValues: 'ALL_OLD',
      });

      expect(result.item).toEqual(returnedItem);
    });

    it('should validate key fields are present', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      await expect(
        testTable.User.put({
          name: 'Test',
          email: 'test@example.com',
          createdAt: '2024-01-01T00:00:00Z',
        } as any)
      ).rejects.toThrow('Missing required partition key field: id');
    });

    it('should validate composite key fields', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id', sk: 'email' },
            schema: TestSchema,
          },
        },
      });

      await expect(
        testTable.User.put({
          id: 'test-id',
          name: 'Test',
          createdAt: '2024-01-01T00:00:00Z',
        } as any)
      ).rejects.toThrow('Missing required sort key field: email');
    });

    it('should marshall ExpressionAttributeValues', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const testItem = createTestEntity();
      const now = new Date().toISOString();
      mockSend.mockResolvedValueOnce({});

      await testTable.User.put(testItem, {
        ConditionExpression: 'attribute_not_exists(#id)',
        ExpressionAttributeNames: { '#id': 'id' },
        ExpressionAttributeValues: { ':now': now },
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            ExpressionAttributeValues: marshall({ ':now': now }),
          }),
        })
      );
    });
  });

  describe('update', () => {
    it('should update an item', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const updatedItem = { ...createTestEntity(), name: 'Updated Name' };
      mockSend.mockResolvedValueOnce({
        Attributes: marshall(updatedItem),
      });

      const result = await testTable.User.update({ id: 'test-id' }, { name: 'Updated Name' });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TableName: 'test-table',
            Key: marshall({ id: 'test-id' }),
            UpdateExpression: expect.stringContaining('SET'),
            ReturnValues: 'ALL_NEW',
          }),
        })
      );
      expect(result.item).toEqual(updatedItem);
    });

    it('should throw error when updates object is empty', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      await expect(testTable.User.update({ id: 'test-id' }, {})).rejects.toThrow(
        'Updates object cannot be empty'
      );
    });

    it('should allow updating non-key fields', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const updatedItem = { ...createTestEntity(), name: 'Updated Name' };
      mockSend.mockResolvedValueOnce({
        Attributes: marshall(updatedItem),
      });

      await testTable.User.update({ id: 'test-id' }, { name: 'Updated Name' });

      const call = mockSend.mock.calls[0][0];
      expect(call.input.UpdateExpression).toContain('#name');
      expect(call.input.UpdateExpression).toContain(':name');
    });

    it('should handle null values as REMOVE operations', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const updatedItem = { ...createTestEntity() };
      const { email, ...itemWithoutEmail } = updatedItem;
      mockSend.mockResolvedValueOnce({
        Attributes: marshall(itemWithoutEmail),
      });

      await testTable.User.update({ id: 'test-id' }, { email: null });

      const call = mockSend.mock.calls[0][0];
      expect(call.input.UpdateExpression).toContain('REMOVE');
      expect(call.input.UpdateExpression).toContain('#email');
    });

    it('should merge ExpressionAttributeNames and ExpressionAttributeValues', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const updatedItem = createTestEntity();
      mockSend.mockResolvedValueOnce({
        Attributes: marshall(updatedItem),
      });

      await testTable.User.update(
        { id: 'test-id' },
        { name: 'New Name' },
        {
          ExpressionAttributeNames: { '#custom': 'customField' },
          ExpressionAttributeValues: { ':custom': 'value' },
          ConditionExpression: '#custom = :custom',
        }
      );

      const call = mockSend.mock.calls[0][0];
      expect(call.input.ExpressionAttributeNames).toHaveProperty('#name');
      expect(call.input.ExpressionAttributeNames).toHaveProperty('#custom');
      expect(call.input.ExpressionAttributeValues).toBeDefined();
    });

    it('should throw error when no attributes returned and ReturnValues is ALL_NEW', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Attributes: undefined,
      });

      await expect(testTable.User.update({ id: 'test-id' }, { name: 'New Name' })).rejects.toThrow(
        'Update operation completed but no attributes were returned'
      );
    });
  });

  describe('delete', () => {
    it('should delete an item', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({});

      const result = await testTable.User.delete({ id: 'test-id' });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TableName: 'test-table',
            Key: marshall({ id: 'test-id' }),
          }),
        })
      );
      expect(result.success).toBe(true);
    });

    it('should support composite keys', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id', sk: 'email' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({});

      await testTable.User.delete({ id: 'test-id', email: 'test@example.com' });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            Key: marshall({ id: 'test-id', email: 'test@example.com' }),
          }),
        })
      );
    });

    it('should marshall ExpressionAttributeValues', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({});

      await testTable.User.delete(
        { id: 'test-id' },
        {
          ConditionExpression: 'attribute_exists(#id)',
          ExpressionAttributeNames: { '#id': 'id' },
          ExpressionAttributeValues: { ':val': 'value' },
        }
      );

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            ExpressionAttributeValues: marshall({ ':val': 'value' }),
          }),
        })
      );
    });
  });

  describe('query', () => {
    it('should query items by partition key', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id', sk: 'email' },
            schema: TestSchema,
          },
        },
      });

      const items = [
        createTestEntity(),
        createTestEntity({ id: 'test-id', email: 'test2@example.com' }),
      ];
      mockSend.mockResolvedValueOnce({
        Items: items.map((item) => marshall(item)),
        LastEvaluatedKey: undefined,
      });

      const result = await testTable.User.query({ pk: 'test-id' });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TableName: 'test-table',
            KeyConditionExpression: expect.stringContaining('#pk = :pk'),
          }),
        })
      );
      expect(result.items).toHaveLength(2);
      expect(result.hasNextPage).toBe(false);
    });

    it('should support sort key conditions', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id', sk: 'email' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [],
        LastEvaluatedKey: undefined,
      });

      await testTable.User.query({
        pk: 'test-id',
        skCondition: '#sk = :sk',
        ExpressionAttributeValues: { ':sk': 'test@example.com' },
      } as any);

      const call = mockSend.mock.calls[0][0];
      expect(call.input.KeyConditionExpression).toContain('AND');
    });

    it('should support index queries', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id', sk: 'email' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [],
        LastEvaluatedKey: undefined,
      });

      await testTable.User.query({
        pk: 'test-id',
        indexName: 'email-index',
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            IndexName: 'email-index',
          }),
        })
      );
    });

    it('should handle pagination', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const lastKey = marshall({ id: 'last-id' });
      mockSend.mockResolvedValueOnce({
        Items: [marshall(createTestEntity())],
        LastEvaluatedKey: lastKey,
      });

      const result = await testTable.User.query({ pk: 'test-id' });

      expect(result.hasNextPage).toBe(true);
      expect(result.nextPageKey).toBeDefined();
    });

    it('should merge ExpressionAttributeValues', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id', sk: 'email' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [],
        LastEvaluatedKey: undefined,
      });

      await testTable.User.query({
        pk: 'test-id',
        skCondition: '#sk = :sk',
        ExpressionAttributeValues: { ':sk': 'test@example.com', ':custom': 'value' },
      } as any);

      const call = mockSend.mock.calls[0][0];
      expect(call.input.ExpressionAttributeValues).toBeDefined();
      const values = unmarshall(call.input.ExpressionAttributeValues!);
      expect(values).toHaveProperty(':pk');
      expect(values).toHaveProperty(':sk');
      expect(values).toHaveProperty(':custom');
    });

    it('should handle exclusiveStartKey', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [],
        LastEvaluatedKey: undefined,
      });

      await testTable.User.query({
        pk: 'test-id',
        exclusiveStartKey: { id: 'last-id' },
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            ExclusiveStartKey: marshall({ id: 'last-id' }),
          }),
        })
      );
    });
  });

  describe('scan', () => {
    it('should scan the table', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      const items = [createTestEntity(), createTestEntity({ id: 'test-id-2' })];
      mockSend.mockResolvedValueOnce({
        Items: items.map((item) => marshall(item)),
        LastEvaluatedKey: undefined,
      });

      const result = await testTable.User.scan();

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TableName: 'test-table',
          }),
        })
      );
      expect(result.items).toHaveLength(2);
      expect(result.hasNextPage).toBe(false);
    });

    it('should handle pagination', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [marshall(createTestEntity())],
        LastEvaluatedKey: { id: { S: 'last-id' } },
      });

      const result = await testTable.User.scan();

      expect(result.hasNextPage).toBe(true);
      expect(result.nextPageKey).toBeDefined();
    });

    it('should handle exclusiveStartKey', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [],
        LastEvaluatedKey: undefined,
      });

      await testTable.User.scan({
        exclusiveStartKey: { id: 'last-id' },
      });

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            ExclusiveStartKey: marshall({ id: 'last-id' }),
          }),
        })
      );
    });

    it('should marshall ExpressionAttributeValues', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [],
        LastEvaluatedKey: undefined,
      });

      await testTable.User.scan({
        FilterExpression: '#name = :name',
        ExpressionAttributeNames: { '#name': 'name' },
        ExpressionAttributeValues: { ':name': 'Test User' },
      } as any);

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            ExpressionAttributeValues: marshall({ ':name': 'Test User' }),
          }),
        })
      );
    });
  });

  describe('paginate', () => {
    it('should paginate through query results', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      // First page
      mockSend.mockResolvedValueOnce({
        Items: [marshall(createTestEntity({ id: '1' }))],
        LastEvaluatedKey: marshall({ id: '1' }),
      });

      // Second page
      mockSend.mockResolvedValueOnce({
        Items: [marshall(createTestEntity({ id: '2' }))],
        LastEvaluatedKey: undefined,
      });

      const pages: TestEntity[][] = [];
      for await (const page of testTable.User.paginate({ pk: 'test-id' })) {
        pages.push(page);
      }

      expect(pages).toHaveLength(2);
      expect(pages[0]).toHaveLength(1);
      expect(pages[1]).toHaveLength(1);
    });

    it('should paginate through scan results', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [marshall(createTestEntity())],
        LastEvaluatedKey: undefined,
      });

      const pages: TestEntity[][] = [];
      for await (const page of testTable.User.paginate({})) {
        pages.push(page);
      }

      expect(pages).toHaveLength(1);
    });

    it('should respect maxPages limit', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      // Return pages with continuation
      mockSend.mockResolvedValue({
        Items: [marshall(createTestEntity())],
        LastEvaluatedKey: marshall({ id: 'next' }),
      });

      const pages: TestEntity[][] = [];
      for await (const page of testTable.User.paginate({ pk: 'test-id' }, { maxPages: 2 })) {
        pages.push(page);
      }

      expect(pages.length).toBeLessThanOrEqual(2);
      expect(mockSend).toHaveBeenCalledTimes(2);
    });

    it('should call onPage callback', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({
        Items: [marshall(createTestEntity())],
        LastEvaluatedKey: undefined,
      });

      const onPage = vi.fn();
      const pages: TestEntity[][] = [];
      for await (const page of testTable.User.paginate({ pk: 'test-id' }, { onPage })) {
        pages.push(page);
      }

      expect(onPage).toHaveBeenCalledTimes(1);
      expect(onPage).toHaveBeenCalledWith(pages[0]);
    });
  });

  describe('transaction', () => {
    it('should execute put transaction', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({});

      await testTable.User.transaction([
        {
          type: 'put',
          item: createTestEntity(),
        },
      ]);

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TransactItems: expect.arrayContaining([
              expect.objectContaining({
                Put: expect.objectContaining({
                  TableName: 'test-table',
                  Item: marshall(createTestEntity()),
                }),
              }),
            ]),
          }),
        })
      );
    });

    it('should execute update transaction', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({});

      await testTable.User.transaction([
        {
          type: 'update',
          key: { id: 'test-id' },
          updateExpression: 'SET #name = :name',
          expressionAttributeNames: { '#name': 'name' },
          expressionAttributeValues: { ':name': 'New Name' },
        },
      ]);

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TransactItems: expect.arrayContaining([
              expect.objectContaining({
                Update: expect.objectContaining({
                  TableName: 'test-table',
                  Key: marshall({ id: 'test-id' }),
                  UpdateExpression: 'SET #name = :name',
                  ExpressionAttributeValues: marshall({ ':name': 'New Name' }),
                }),
              }),
            ]),
          }),
        })
      );
    });

    it('should execute delete transaction', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({});

      await testTable.User.transaction([
        {
          type: 'delete',
          key: { id: 'test-id' },
        },
      ]);

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TransactItems: expect.arrayContaining([
              expect.objectContaining({
                Delete: expect.objectContaining({
                  TableName: 'test-table',
                  Key: marshall({ id: 'test-id' }),
                }),
              }),
            ]),
          }),
        })
      );
    });

    it('should execute mixed transaction operations', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({});

      await testTable.User.transaction([
        {
          type: 'put',
          item: createTestEntity({ id: '1' }),
        },
        {
          type: 'update',
          key: { id: '2' },
          updateExpression: 'SET #name = :name',
          expressionAttributeNames: { '#name': 'name' },
          expressionAttributeValues: { ':name': 'Updated' },
        },
        {
          type: 'delete',
          key: { id: '3' },
        },
      ]);

      expect(mockSend).toHaveBeenCalledWith(
        expect.objectContaining({
          input: expect.objectContaining({
            TransactItems: expect.arrayContaining([
              expect.objectContaining({ Put: expect.any(Object) }),
              expect.objectContaining({ Update: expect.any(Object) }),
              expect.objectContaining({ Delete: expect.any(Object) }),
            ]),
          }),
        })
      );
    });

    it('should support condition expressions', async () => {
      const testTable = table(mockClient, {
        tableName: 'test-table',
        schemas: {
          User: {
            key: { pk: 'id' },
            schema: TestSchema,
          },
        },
      });

      mockSend.mockResolvedValueOnce({});

      await testTable.User.transaction([
        {
          type: 'put',
          item: createTestEntity(),
          conditionExpression: 'attribute_not_exists(#id)',
        },
        {
          type: 'delete',
          key: { id: 'test-id' },
          conditionExpression: 'attribute_exists(#id)',
        },
      ]);

      const call = mockSend.mock.calls[0][0];
      expect(call.input.TransactItems[0].Put.ConditionExpression).toBe('attribute_not_exists(#id)');
      expect(call.input.TransactItems[1].Delete.ConditionExpression).toBe('attribute_exists(#id)');
    });
  });
});
