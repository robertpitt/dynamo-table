# itty-repo

A lightweight, type-safe DynamoDB repository layer for TypeScript that uses Standard Schema V1 for type inference. Designed to complement [itty-spec](https://robertpitt.github.io/itty-spec/) in serverless API development.

## Features

- **Type-safe**: Full TypeScript inference from Standard Schema schemas (Zod, Valibot, etc.)
- **Lightweight**: No runtime validation overhead - types only
- **Flexible**: Supports both single-entity tables with GSIs and multi-entity single-table designs
- **Complete**: All DynamoDB operations (get, put, update, delete, query, scan, batch, transact)

## Installation

```bash
pnpm add itty-repo @standard-schema/spec aws-sdk
# or
npm install itty-repo @standard-schema/spec aws-sdk
# or
yarn add itty-repo @standard-schema/spec aws-sdk
```

## Quick Start

### Single-Entity Table

```typescript
import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { Repo } from 'itty-repo';
import { z } from 'zod';

// Define your entity schema
const UserSchema = z.object({
  pk: z.string(), // Partition key
  name: z.string(),
  email: z.string().email(),
  createdAt: z.string(),
});

// Create repository
const client = new DocumentClient({ region: 'us-east-1' });
const userRepo = new Repo(client, {
  tableName: 'Users',
  schema: UserSchema,
  gsis: [
    { name: 'EmailIndex', partitionKey: 'email' },
  ],
});

// Use the repository
const user = await userRepo.get('USER#123');
await userRepo.put({
  pk: 'USER#123',
  name: 'John Doe',
  email: 'john@example.com',
  createdAt: new Date().toISOString(),
});

// Query by GSI
const usersByEmail = await userRepo.query({
  pk: 'john@example.com',
  indexName: 'EmailIndex',
});
```

### Multi-Entity Single-Table Design

```typescript
import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { Repo } from 'itty-repo';
import { z } from 'zod';

// Define entity schema (entity type inferred from PK pattern)
const EntitySchema = z.object({
  pk: z.string(), // e.g., "USER#123" or "ORDER#456"
  sk: z.string(), // e.g., "PROFILE" or "ORDER#789"
  name: z.string().optional(),
  email: z.string().email().optional(),
  orderDate: z.string().optional(),
  // ... other fields
});

const client = new DocumentClient({ region: 'us-east-1' });
const repo = new Repo(client, {
  tableName: 'MainTable',
  schema: EntitySchema,
});

// Store user profile
await repo.put({
  pk: 'USER#123',
  sk: 'PROFILE',
  name: 'John Doe',
  email: 'john@example.com',
});

// Store order
await repo.put({
  pk: 'USER#123',
  sk: 'ORDER#456',
  orderDate: '2024-01-01',
});

// Query all items for a user
const userItems = await repo.query({
  pk: 'USER#123',
});

// Query orders for a user (using begins_with)
import { beginsWith } from 'itty-repo';
const orders = await repo.query({
  pk: 'USER#123',
  skCondition: beginsWith('ORDER#'),
});
```

## API Reference

### Repo Class

#### Constructor

```typescript
new Repo(client: DocumentClient, config: RepoConfig<T>)
```

**Config Options:**
- `tableName: string` - DynamoDB table name
- `schema: StandardSchemaV1` - Standard Schema V1 schema
- `gsis?: GSIDefinition[]` - Global Secondary Index definitions
- `pkField?: string` - Partition key field name (default: `'pk'`)
- `skField?: string` - Sort key field name (default: `'sk'`)

#### Methods

##### `get(pk: string, sk?: string, options?: GetOptions): Promise<GetResult<T>>`

Get a single item by partition key and optional sort key.

```typescript
const result = await repo.get('USER#123');
// result.item is T | null
```

##### `put(item: EntityInput<T>, options?: PutOptions): Promise<PutResult>`

Put (create or replace) an item.

```typescript
await repo.put({
  pk: 'USER#123',
  name: 'John Doe',
});
```

##### `update(pk: string, sk: string | undefined, options: UpdateOptions): Promise<UpdateResult>`

Update an item using UpdateExpression.

```typescript
await repo.update('USER#123', undefined, {
  updateExpression: 'SET #name = :name',
  expressionAttributeNames: { '#name': 'name' },
  expressionAttributeValues: { ':name': 'Jane Doe' },
});
```

##### `delete(pk: string, sk?: string, options?: DeleteOptions): Promise<DeleteResult>`

Delete an item by partition key and optional sort key.

```typescript
await repo.delete('USER#123', 'PROFILE');
```

##### `query(options: QueryOptions): Promise<QueryResult<T>>`

Query items by partition key (and optional sort key condition).

```typescript
// Simple query
const result = await repo.query({ pk: 'USER#123' });

// Query with sort key condition
import { beginsWith, between } from 'itty-repo';
const result = await repo.query({
  pk: 'USER#123',
  skCondition: beginsWith('ORDER#'),
});

// Query by GSI
const result = await repo.query({
  pk: 'user@example.com',
  indexName: 'EmailIndex',
});
```

**Query Options:**
- `pk: string` - Partition key value
- `skCondition?: SKCondition` - Sort key condition
- `indexName?: string` - GSI name for querying
- `filterExpression?: string` - Filter expression
- `limit?: number` - Limit number of items
- `consistentRead?: boolean` - Use consistent read
- `scanIndexForward?: boolean` - Scan index forward
- `exclusiveStartKey?: Record<string, unknown>` - Pagination key
- `projectionExpression?: string` - Projection expression
- `returnConsumedCapacity?: 'INDEXES' | 'TOTAL' | 'NONE'`

##### `scan(options?: ScanOptions): Promise<ScanResult<T>>`

Scan the entire table.

```typescript
const result = await repo.scan({
  filterExpression: '#status = :active',
  expressionAttributeNames: { '#status': 'status' },
  expressionAttributeValues: { ':status': 'active' },
});
```

##### `batchGet(keys: Array<{ pk: string; sk?: string }>, options?: BatchGetOptions): Promise<BatchGetResult<T>>`

Batch get multiple items.

```typescript
const result = await repo.batchGet([
  { pk: 'USER#123' },
  { pk: 'USER#456', sk: 'PROFILE' },
]);
```

##### `batchWrite(requests: Array<PutRequest | DeleteRequest>, options?: BatchWriteOptions): Promise<BatchWriteResult>`

Batch write (put/delete) multiple items.

```typescript
await repo.batchWrite([
  { type: 'put', item: { pk: 'USER#123', name: 'John' } },
  { type: 'delete', pk: 'USER#456', sk: 'PROFILE' },
]);
```

##### `transactWrite(requests: Array<TransactRequest>, options?: TransactWriteOptions): Promise<TransactWriteResult>`

Transact write multiple items.

```typescript
await repo.transactWrite([
  {
    type: 'put',
    item: { pk: 'USER#123', name: 'John' },
    conditionExpression: 'attribute_not_exists(pk)',
  },
  {
    type: 'update',
    pk: 'USER#456',
    updateExpression: 'SET #count = #count + :inc',
    expressionAttributeNames: { '#count': 'count' },
    expressionAttributeValues: { ':inc': 1 },
  },
]);
```

### Query Helpers

```typescript
import {
  beginsWith,
  between,
  equals,
  lessThan,
  lessThanOrEqual,
  greaterThan,
  greaterThanOrEqual,
} from 'itty-repo';

// Use in query options
await repo.query({
  pk: 'USER#123',
  skCondition: beginsWith('ORDER#'),
});

await repo.query({
  pk: 'USER#123',
  skCondition: between('2024-01-01', '2024-12-31'),
});
```

### Key Utilities

```typescript
import {
  extractPartitionKey,
  extractSortKey,
  extractKeys,
  inferEntityType,
  buildKey,
} from 'itty-repo';

const entity = { pk: 'USER#123', sk: 'PROFILE', name: 'John' };
const pk = extractPartitionKey(entity); // 'USER#123'
const sk = extractSortKey(entity); // 'PROFILE'
const keys = extractKeys(entity); // { pk: 'USER#123', sk: 'PROFILE' }
const entityType = inferEntityType('USER#123'); // 'User'
const key = buildKey('USER#123', 'PROFILE'); // { pk: 'USER#123', sk: 'PROFILE' }
```

## Type Safety

`itty-repo` provides full type inference from your Standard Schema schemas. All methods are fully typed, and **all TypeScript type information is preserved**, including:

- **Branded types** (e.g., `string & { __brand: 'UUID' }`)
- **Literal types** (e.g., `'pending' | 'active' | 'inactive'`)
- **Template literal types** (e.g., `` `${string}-${string}-${string}-${string}-${string}` ``)
- **Union types, intersection types, and all other TypeScript types**

### Basic Type Inference

```typescript
const UserSchema = z.object({
  pk: z.string(),
  name: z.string(),
  email: z.string().email(),
});

const repo = new Repo(client, { tableName: 'Users', schema: UserSchema });

// TypeScript knows the exact shape!
const user = await repo.get('USER#123');
// user.item is { pk: string; name: string; email: string } | null

await repo.put({
  pk: 'USER#123',
  name: 'John',
  email: 'john@example.com',
  // TypeScript error if you add invalid fields!
});
```

### Branded Types and Literal Types

Branded types, literal types, and template literal types are fully preserved through the type inference chain:

```typescript
// Define branded types
type UUID = string & { __brand: 'UUID' };
type Email = string & { __brand: 'Email' };
type Status = 'pending' | 'active' | 'inactive';

// Create schema with branded types (using Standard Schema V1)
const UserSchema = {
  '~standard': {
    input: {} as { pk: string; userId: string; email: string; status: string },
    output: {} as { pk: string; userId: UUID; email: Email; status: Status },
    validate: (v: unknown) => ({ value: v, issues: [] }),
    parse: (v: unknown) => ({ value: v, issues: [] }),
  },
} as StandardSchemaV1<{
  input: { pk: string; userId: string; email: string; status: string };
  output: { pk: string; userId: UUID; email: Email; status: Status };
}>;

const repo = new Repo(client, { tableName: 'Users', schema: UserSchema });

// All branded types are preserved!
const result = await repo.get('USER#123');
if (result.item) {
  const userId: UUID = result.item.userId; // ✅ UUID type preserved
  const email: Email = result.item.email;   // ✅ Email type preserved
  const status: Status = result.item.status; // ✅ Status literal type preserved
}

// Query results also preserve types
const queryResult = await repo.query({ pk: 'USER#123' });
queryResult.items.forEach((item) => {
  const userId: UUID = item.userId; // ✅ Types preserved in arrays too
});
```

See `examples/branded-types.ts` for a complete example with branded types and template literal types.

## Integration with itty-spec

You can share schemas between `itty-spec` contracts and `itty-repo`:

```typescript
import { createContract } from 'itty-spec';
import { Repo } from 'itty-repo';
import { z } from 'zod';

// Shared schema
const UserSchema = z.object({
  id: z.string().uuid(),
  name: z.string(),
  email: z.string().email(),
});

// Use in itty-spec contract
const contract = createContract({
  getUser: {
    path: '/users/:id',
    method: 'GET',
    pathParams: z.object({ id: z.string().uuid() }),
    responses: {
      200: {
        'application/json': {
          body: UserSchema,
        },
      },
    },
  },
});

// Use in itty-repo (with PK/SK)
const UserWithKeysSchema = UserSchema.extend({
  pk: z.string(),
  sk: z.string(),
});

const userRepo = new Repo(client, {
  tableName: 'Users',
  schema: UserWithKeysSchema,
});
```

## License

MIT

