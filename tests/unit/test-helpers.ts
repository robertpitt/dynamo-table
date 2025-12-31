import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import { vi } from 'vitest';

/**
 * Creates a mock DynamoDB client for testing
 */
export function createMockDynamoDBClient() {
  const mockSend = vi.fn();
  const client = {
    send: mockSend,
  } as unknown as DynamoDBClient;

  return { client, mockSend };
}

/**
 * Helper to create a test entity schema
 */
export function createTestSchema() {
  return {
    id: 'string',
    name: 'string',
    email: 'string',
    createdAt: 'string',
  } as const;
}

/**
 * Helper to create a test entity
 */
export function createTestEntity(overrides?: Partial<TestEntity>): TestEntity {
  return {
    id: 'test-id',
    name: 'Test User',
    email: 'test@example.com',
    createdAt: '2024-01-01T00:00:00Z',
    ...overrides,
  };
}

export type TestEntity = {
  id: string;
  name: string;
  email: string;
  createdAt: string;
};

/**
 * Helper to create a DynamoDB item response
 */
export function createDynamoDBItem(item: Record<string, unknown>) {
  return marshall(item);
}

/**
 * Helper to unmarshall a DynamoDB item
 */
export function unmarshallDynamoDBItem(item: ReturnType<typeof marshall>) {
  return unmarshall(item);
}
