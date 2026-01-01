# itty-repo

A lightweight, type-safe DynamoDB repository layer for TypeScript that uses Valibot for type inference. Designed for a clean, intuitive developer experience.

## Features

- **Type-safe**: Full TypeScript inference from Valibot schemas
- **Lightweight**: Minimal runtime overhead
- **Intuitive API**: Query by field names, not pk/sk abstractions
- **Complete**: All DynamoDB operations (get, put, update, delete, query, queryIndex, scan, batchGet, transact)
- **Helper Functions**: Chainable filter and condition builders
- **Custom Methods**: Add repository pattern methods to table instances

## Installation

```bash
pnpm add itty-repo valibot @aws-sdk/client-dynamodb
# or
npm install itty-repo valibot @aws-sdk/client-dynamodb
# or
yarn add itty-repo valibot @aws-sdk/client-dynamodb
```

## Quick Start

```typescript
import * as v from 'valibot';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { table, key, index, filter, condition } from 'itty-repo';

// Initialize the DynamoDB client
const client = new DynamoDBClient({ region: 'us-east-1' });

// Define your schema using Valibot
const UserSchema = v.object({
  id: v.pipe(v.string(), v.uuid()),
  name: v.string(),
  email: v.string(),
  createdAt: v.string(),
  updatedAt: v.string(),
});

// Create a table
const users = table(client, {
  tableName: process.env.USERS_TABLE_NAME!,
  key: key('id'), // Single partition key
  schema: UserSchema,
  indexes: [
    index('emailIndex', key('email')), // GSI on email
  ],
});

// Use the table
const user = await users.get({ id: 'user-123' });
await users.put({
  id: crypto.randomUUID(),
  name: 'John Doe',
  email: 'john@example.com',
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
});

// Query by index
const userByEmail = await users.queryIndex('emailIndex', { email: 'john@example.com' });
```

## API Reference

### Table Factory

```typescript
table(client: DynamoDBClient, config: TableConfig<Schema>): Table<Schema>
```

**Config Options:**
- `tableName: string` - DynamoDB table name
- `key: KeyConfig` - Key configuration (use `key()` helper)
- `schema: BaseSchema` - Valibot schema
- `ttl?: string` - Optional TTL field name
- `indexes?: IndexConfig[]` - Optional index configurations
- `methods?: Record<string, Function>` - Optional custom methods

### Helper Functions

#### `key(pk: string, sk?: string): KeyConfig`

Creates a key configuration.

```typescript
key('id')                    // Single partition key
key('orgId', 'id')           // Composite key
key('userId', 'createdAt')   // Composite key with sort key
```

#### `index(name: string, keyConfig: KeyConfig, options?: IndexOptions): IndexConfig`

Creates an index configuration.

```typescript
index('emailIndex', key('email'))
index('userOrdersIndex', key('userId', 'createdAt'), { projection: 'KEYS_ONLY' })
index('statusIndex', key('status', 'createdAt'), {
  projection: { include: ['id', 'status', 'createdAt'] }
})
```

#### `filter(builder: (f: FilterBuilder) => string): FilterExpression`

Builds filter expressions for query/scan operations.

```typescript
filter(f => f.eq('status', 'active'))
filter(f => f.and(
  f.eq('userId', '123'),
  f.between('createdAt', startDate, endDate)
))
```

#### `condition(builder: (c: ConditionBuilder) => string): ConditionExpression`

Builds condition expressions for put/update/delete operations.

```typescript
condition(c => c.notExists('id'))
condition(c => c.and(
  c.exists('id'),
  c.eq('status', 'active')
))
```

### Table Methods

#### `get(key: KeyInput<Entity, Key>, options?: GetOptions): Promise<GetResult<Entity>>`

Get a single item by its key.

```typescript
const result = await users.get({ id: 'user-123' });
// result.item is Entity | null
```

#### `batchGet(keys: KeyInput<Entity, Key>[], options?: BatchGetOptions): Promise<{ items: Entity[] }>`

Batch get multiple items.

```typescript
const result = await users.batchGet([
  { id: 'user-1' },
  { id: 'user-2' },
]);
// result.items is Entity[]
```

#### `put(item: Entity, options?: PutOptions): Promise<PutResult<Entity>>`

Put (create or replace) an item.

```typescript
await users.put({
  id: crypto.randomUUID(),
  name: 'John Doe',
  email: 'john@example.com',
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
});

// Conditional put (only if doesn't exist)
await users.put(item, {
  condition: condition(c => c.notExists('id')),
});
```

#### `update(key: KeyInput<Entity, Key>, updates: UpdateInput<Entity, Key>, options?: UpdateOptions): Promise<UpdateResult<Entity>>`

Update an item by its key.

```typescript
await users.update(
  { id: 'user-123' },
  { name: 'Jane Doe', updatedAt: new Date().toISOString() }
);

// Conditional update
await users.update(
  { id: 'user-123' },
  { status: 'active' },
  {
    condition: condition(c => c.eq('status', 'pending')),
  }
);
```

#### `delete(key: KeyInput<Entity, Key>, options?: DeleteOptions): Promise<DeleteResult>`

Delete an item by its key.

```typescript
await users.delete({ id: 'user-123' });

// Conditional delete
await users.delete(
  { id: 'user-123' },
  {
    condition: condition(c => c.exists('id')),
  }
);
```

#### `query(keyFields: Partial<Entity>, options?: QueryOptions<Entity>): Promise<PagedResult<Entity>>`

Query items by field names (automatically mapped to pk/sk).

```typescript
// Query by partition key
const result = await users.query({ id: 'user-123' });

// Query with filter
const result = await users.query(
  { id: 'user-123' },
  {
    filter: filter(f => f.gt('createdAt', '2025-01-01')),
  }
);

// Query with pagination
const page1 = await users.query({ id: 'user-123' }, { limit: 10 });
const page2 = await users.query(
  { id: 'user-123' },
  { limit: 10, exclusiveStartKey: page1.nextPageKey }
);
```

#### `queryIndex(indexName: string, keyFields: Partial<Entity>, options?: QueryOptions<Entity>): Promise<PagedResult<Entity>>`

Query a Global Secondary Index or Local Secondary Index.

```typescript
// Query by index
const result = await users.queryIndex('emailIndex', { email: 'john@example.com' });

// Query index with filter
const result = await users.queryIndex(
  'userOrdersIndex',
  { userId: 'user-123' },
  {
    filter: filter(f => f.between('createdAt', startDate, endDate)),
  }
);
```

#### `scan(options?: ScanOptions): Promise<PagedResult<Entity>>`

Scan the entire table.

```typescript
const result = await users.scan();

// Scan with filter
const result = await users.scan({
  filter: filter(f => f.eq('status', 'active')),
  limit: 100,
});
```

#### `paginate(options, paginateOptions?): AsyncGenerator<Entity[], void, unknown>`

Paginate through query or scan results.

```typescript
for await (const page of users.paginate(
  { id: 'user-123' },
  { maxPages: 10, onPage: (items) => console.log(`Got ${items.length} items`) }
)) {
  // Process each page
  console.log(page);
}
```

#### `transaction(requests: TransactWriteRequest<Entity>[]): Promise<void>`

Transaction write multiple items.

```typescript
await users.transaction([
  { type: 'put', item: { id: '1', name: 'User 1' } },
  { type: 'update', key: { id: '2' }, updateExpression: 'SET #name = :name', ... },
  { type: 'delete', key: { id: '3' } },
]);
```

## Filter Builder Methods

The filter builder supports the following methods:

- `eq(attribute, value)` - Equality
- `ne(attribute, value)` - Inequality
- `lt(attribute, value)` - Less than
- `lte(attribute, value)` - Less than or equal
- `gt(attribute, value)` - Greater than
- `gte(attribute, value)` - Greater than or equal
- `beginsWith(attribute, value)` - Begins with
- `contains(attribute, value)` - Contains
- `between(attribute, start, end)` - Between
- `in(attribute, values[])` - In array
- `exists(attribute)` - Attribute exists
- `notExists(attribute)` - Attribute does not exist
- `size(attribute)` - Size of attribute
- `sizeGt(attribute, value)` - Size greater than
- `sizeLt(attribute, value)` - Size less than
- `and(...conditions)` - Logical AND
- `or(...conditions)` - Logical OR
- `not(condition)` - Logical NOT

## Condition Builder Methods

The condition builder supports all filter builder methods plus:

- `attributeType(attribute, type)` - Attribute type check
- `sizeGte(attribute, value)` - Size greater than or equal
- `sizeLte(attribute, value)` - Size less than or equal

## Custom Methods

Add custom repository methods to your table instance:

```typescript
const users = table(client, {
  tableName: 'users',
  key: key('id'),
  schema: UserSchema,
  methods: {
    getActiveUsers: (table) => {
      return async () => {
        return table.scan({
          filter: filter(f => f.eq('status', 'active')),
        });
      };
    },
    getUserByEmail: (table) => {
      return async (email: string) => {
        const result = await table.queryIndex('emailIndex', { email });
        return result.items[0] ?? null;
      };
    },
  },
});

// Use custom methods
const activeUsers = await users.getActiveUsers();
const user = await users.getUserByEmail('john@example.com');
```

## Type Safety

`itty-repo` provides full type inference from your Valibot schemas. All methods are fully typed, and TypeScript type information is preserved, including:

- **Branded types** (e.g., `string & { __brand: 'UUID' }`)
- **Literal types** (e.g., `'pending' | 'active' | 'inactive'`)
- **Template literal types**
- **Union types, intersection types, and all other TypeScript types**

## License

MIT
