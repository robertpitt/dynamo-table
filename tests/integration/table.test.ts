import { describe, it, expect, vi, beforeEach } from 'vitest';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { table } from '../../src/table.js';
import type { StandardSchemaV1 } from '@standard-schema/spec';
import * as v from 'valibot';

// Mock DynamoDBDocumentClient
const mockSend = vi.fn();

vi.mock('@aws-sdk/lib-dynamodb', () => {
  // Define mock command classes inside the factory
  class MockGetCommand {
    constructor(public input: unknown) {}
  }

  class MockPutCommand {
    constructor(public input: unknown) {}
  }

  class MockUpdateCommand {
    constructor(public input: unknown) {}
  }

  class MockDeleteCommand {
    constructor(public input: unknown) {}
  }

  class MockQueryCommand {
    constructor(public input: unknown) {}
  }

  class MockScanCommand {
    constructor(public input: unknown) {}
  }

  class MockBatchGetCommand {
    constructor(public input: unknown) {}
  }

  class MockBatchWriteCommand {
    constructor(public input: unknown) {}
  }

  class MockTransactWriteCommand {
    constructor(public input: unknown) {}
  }

  return {
    DynamoDBDocumentClient: {
      from: vi.fn(() => ({
        send: mockSend,
      })),
    },
    GetCommand: MockGetCommand,
    PutCommand: MockPutCommand,
    UpdateCommand: MockUpdateCommand,
    DeleteCommand: MockDeleteCommand,
    QueryCommand: MockQueryCommand,
    ScanCommand: MockScanCommand,
    BatchGetCommand: MockBatchGetCommand,
    BatchWriteCommand: MockBatchWriteCommand,
    TransactWriteCommand: MockTransactWriteCommand,
  };
});

describe('table', () => {
  let mockClient: DynamoDBClient;
  let UserSchema: StandardSchemaV1;
  let UserEmailIndexSchema: StandardSchemaV1;

  beforeEach(() => {
    mockClient = {} as DynamoDBClient;
    mockSend.mockClear();

    // Create mock schemas
    UserSchema = v.object({
      id: v.string(),
      name: v.string(),
      email: v.string(),
      createdAt: v.string(),
      updatedAt: v.string(),
    }) as StandardSchemaV1;

    UserEmailIndexSchema = v.object({
      email: v.string(),
      id: v.string(),
    }) as StandardSchemaV1;
  });

  describe('table creation', () => {
    it('should create a table with entity repositories', () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
          UserEmailIndex: { key: { pk: 'email', sk: 'id' }, schema: UserEmailIndexSchema },
        },
      });

      expect(usersTable.User).toBeDefined();
      expect(usersTable.UserEmailIndex).toBeDefined();
    });
  });

  describe('get', () => {
    it('should get an item by partition key', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      const mockItem = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com',
        createdAt: '2024-01-01',
        updatedAt: '2024-01-01',
      };

      mockSend.mockResolvedValueOnce({ Item: mockItem });

      const result = await usersTable.User.get({ id: '123' });

      expect(result.item).toEqual(mockItem);
      expect(mockSend).toHaveBeenCalled();
    });

    it('should get an item by partition and sort key', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          UserEmailIndex: { key: { pk: 'email', sk: 'id' }, schema: UserEmailIndexSchema },
        },
      });

      const mockItem = {
        email: 'john@example.com',
        id: '123',
      };

      mockSend.mockResolvedValueOnce({ Item: mockItem });

      const result = await usersTable.UserEmailIndex.get({ email: 'john@example.com', id: '123' });

      expect(result.item).toEqual(mockItem);
    });

    it('should return null when item not found', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      mockSend.mockResolvedValueOnce({});

      const result = await usersTable.User.get({ id: '123' });

      expect(result.item).toBeNull();
    });
  });

  describe('put', () => {
    it('should put an item', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      const item = {
        id: '123',
        name: 'John Doe',
        email: 'john@example.com',
        createdAt: '2024-01-01',
        updatedAt: '2024-01-01',
      };

      mockSend.mockResolvedValueOnce({});

      const result = await usersTable.User.put(item);

      expect(result.item).toEqual(item);
      expect(mockSend).toHaveBeenCalled();
    });
  });

  describe('query', () => {
    it('should query items by partition key', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      const mockItems = [
        {
          id: '123',
          name: 'John Doe',
          email: 'john@example.com',
          createdAt: '2024-01-01',
          updatedAt: '2024-01-01',
        },
      ];

      mockSend.mockResolvedValueOnce({
        Items: mockItems,
        Count: 1,
      });

      const result = await usersTable.User.query({ pk: 'USER#123' });

      expect(result.items).toEqual(mockItems);
      expect(result.hasNextPage).toBe(false);
    });

    it('should query with pagination', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      const lastEvaluatedKey = { id: '123' };
      mockSend.mockResolvedValueOnce({
        Items: [],
        Count: 0,
        LastEvaluatedKey: lastEvaluatedKey,
      });

      const result = await usersTable.User.query({ pk: 'USER#123' });

      expect(result.hasNextPage).toBe(true);
      expect(result.nextPageKey).toBeDefined();
    });
  });

  describe('scan', () => {
    it('should scan the table', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      const mockItems = [
        {
          id: '123',
          name: 'John Doe',
          email: 'john@example.com',
          createdAt: '2024-01-01',
          updatedAt: '2024-01-01',
        },
      ];

      mockSend.mockResolvedValueOnce({
        Items: mockItems,
        Count: 1,
      });

      const result = await usersTable.User.scan();

      expect(result.items).toEqual(mockItems);
    });
  });

  describe('delete', () => {
    it('should delete an item', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      mockSend.mockResolvedValueOnce({});

      const result = await usersTable.User.delete({ id: '123' });

      expect(result.success).toBe(true);
      expect(mockSend).toHaveBeenCalled();
    });
  });

  describe('paginate', () => {
    it('should paginate through query results', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      const mockItems1 = [
        {
          id: '123',
          name: 'John Doe',
          email: 'john@example.com',
          createdAt: '2024-01-01',
          updatedAt: '2024-01-01',
        },
      ];

      const mockItems2 = [
        {
          id: '456',
          name: 'Jane Doe',
          email: 'jane@example.com',
          createdAt: '2024-01-01',
          updatedAt: '2024-01-01',
        },
      ];

      mockSend
        .mockResolvedValueOnce({
          Items: mockItems1,
          Count: 1,
          LastEvaluatedKey: { id: '123' },
        })
        .mockResolvedValueOnce({
          Items: mockItems2,
          Count: 1,
        });

      const pages: unknown[][] = [];
      for await (const page of usersTable.User.paginate({ pk: 'USER#123' })) {
        pages.push(page);
      }

      expect(pages).toHaveLength(2);
      expect(pages[0]).toEqual(mockItems1);
      expect(pages[1]).toEqual(mockItems2);
    });
  });

  describe('transaction', () => {
    it('should execute a transaction', async () => {
      const usersTable = table(mockClient, {
        tableName: 'Users',
        schemas: {
          User: { key: { pk: 'id' }, schema: UserSchema },
        },
      });

      mockSend.mockResolvedValueOnce({});

      await usersTable.User.transaction([
        {
          type: 'put',
          item: {
            id: '123',
            name: 'John Doe',
            email: 'john@example.com',
            createdAt: '2024-01-01',
            updatedAt: '2024-01-01',
          },
        },
      ]);

      expect(mockSend).toHaveBeenCalled();
    });
  });
});
