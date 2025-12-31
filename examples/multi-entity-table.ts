/**
 * Example: Multi-Entity Table with Standard Schema
 *
 * This example demonstrates using itty-repo with a multi-entity table design
 * where different entities share the same DynamoDB table but have different schemas.
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { table } from '../src/index.js';
import * as v from 'valibot';

// Define entity schemas using Valibot (or any Standard Schema compatible library)
// Valibot schemas are already StandardSchemaV1 compatible, so no cast is needed
const UserEntity = v.object({
  id: v.string(),
  name: v.string(),
  email: v.string(),
  createdAt: v.string(),
  updatedAt: v.string(),
});

const UserEmailIndexEntity = v.object({
  email: v.string(),
  id: v.string(),
});

// Create table with multiple entity schemas
// Note: Use 'as const' on the key config to preserve literal types for better type inference
// This ensures that 'id' and 'email' are inferred as literal types, not widened to 'string'
const usersTable = table(new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' }), {
  tableName: process.env.USERS_TABLE_NAME!,
  schemas: {
    User: { key: { pk: 'id' } as const, schema: UserEntity },
    UserEmailIndex: { key: { pk: 'email', sk: 'id' } as const, schema: UserEmailIndexEntity },
  },
});

usersTable.User.get({ id: 'test' });
usersTable.UserEmailIndex.get({ email: 'test', id: 'test' });
usersTable.User.query({
  pk: 'test',
});

/**
 * Create a new user
 */
export async function createUser(userData: { id: string; name: string; email: string }) {
  const now = new Date().toISOString();

  return await usersTable.User.put({
    id: userData.id,
    name: userData.name,
    email: userData.email,
    createdAt: now,
    updatedAt: now,
  });
}

/**
 * Get a user by ID
 */
export async function getUserById(id: string) {
  const result = await usersTable.User.get({ id });
  return result.item;
}

/**
 * Get a user by email (using email index entity)
 */
export async function getUserByEmail(email: string) {
  const result = await usersTable.UserEmailIndex.query({
    pk: email,
    limit: 1,
  });

  return result.items[0] ?? null;
}

/**
 * Update a user's name
 */
export async function updateUserName(id: string, newName: string) {
  return await usersTable.User.update(
    { id },
    {
      name: newName,
      updatedAt: new Date().toISOString(),
    }
  );
}

/**
 * Delete a user
 */
export async function deleteUser(id: string) {
  return await usersTable.User.delete({ id });
}

/**
 * Query users (example - would need appropriate GSI or table structure)
 */
export async function queryUsers() {
  // Note: This would require a GSI or the table to have a queryable structure
  // This is just an example of the API
  const result = await usersTable.User.scan({
    limit: 10,
  });

  return result.items;
}

// Example usage (commented out for demonstration)
/*
async function main() {
  // Create a user
  await createUser({
    id: '123',
    name: 'John Doe',
    email: 'john@example.com',
  });

  // Get user by ID
  const user = await getUserById('123');
  console.log('User:', user);

  // Get user by email
  const userByEmail = await getUserByEmail('john@example.com');
  console.log('User by email:', userByEmail);

  // Update user name
  await updateUserName('123', 'Jane Doe');

  // Query users
  const users = await queryUsers();
  console.log('Users:', users);

  // Delete user
  await deleteUser('123');
}
*/
