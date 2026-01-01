# itty-repo

A lightweight, type-safe DynamoDB repository layer for TypeScript that uses Valibot for type inference. Designed for a clean, intuitive developer experience.

## Features

- **Type-safe**: Full TypeScript inference from Valibot schemas (StandardSchemaV1)
- **Lightweight**: Minimal runtime overhead
- **Intuitive API**: Query by field names, not pk/sk abstractions
- **Complete**: All DynamoDB operations (get, put, update, delete, query, queryIndex, scan, batchGet, transaction)
- **Expression Builders**: Type-safe filter and condition expression builders with nested attribute path support
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
import { table } from 'itty-repo';

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
  key: { pk: 'id' }, // Single partition key
  schema: UserSchema,
  indexes: {
    emailIndex: { key: { pk: 'email' }, projection: 'KEYS_ONLY' }, // GSI on email
  },
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
- `key: KeyConfig` - Key configuration object with `pk` and optional `sk` properties
- `schema: StandardSchemaV1` - Valibot schema (conforming to StandardSchemaV1)
- `ttl?: string` - Optional TTL field name
- `indexes?: Record<string, IndexConfig>` - Optional index configurations (object with index names as keys)
- `methods?: Record<string, MethodFactory>` - Optional custom method factories

### Key Configuration

Key configurations are plain objects with `pk` (partition key) and optional `sk` (sort key) properties:

```typescript
{ pk: 'id' }                           // Single partition key
{ pk: 'orgId', sk: 'id' }              // Composite key
{ pk: 'userId', sk: 'createdAt' }      // Composite key with sort key
```

### Index Configuration

Index configurations are objects with `key` and `projection` properties:

```typescript
{
  emailIndex: { 
    key: { pk: 'email' }, 
    projection: 'KEYS_ONLY' 
  }
}

{
  userOrdersIndex: { 
    key: { pk: 'userId', sk: 'createdAt' }, 
    projection: 'KEYS_ONLY' 
  }
}

{
  statusIndex: { 
    key: { pk: 'status', sk: 'createdAt' }, 
    projection: { include: ['id', 'status', 'createdAt'] } 
  }
}
```

**Projection options:**
- `'KEYS_ONLY'` - Only key attributes
- `'ALL'` - All attributes
- `{ include: string[] }` - Specific attributes to include

### Filter Expressions

Filter expressions are built using builder functions passed directly to `filter` option:

```typescript
filter: (f) => f.eq('status', 'active')

filter: (f) => f.and(
  f.eq('userId', '123'),
  f.between('createdAt', startDate, endDate)
)
```

### Condition Expressions

Condition expressions are built using builder functions passed directly to `condition` option:

```typescript
condition: (c) => c.notExists('id')

condition: (c) => c.and(
  c.exists('id'),
  c.eq('status', 'active')
)
```

### Table Methods

#### `get(key: KeyInput<Entity, Key>, options?: GetOptions): Promise<Entity | undefined>`

Get a single item by its key.

```typescript
const user = await users.get({ id: 'user-123' });
// user is Entity | undefined

// Get with consistent read
const user = await users.get({ id: 'user-123' }, { consistentRead: true });

// Get with projection (only return specific attributes)
const user = await users.get({ id: 'user-123' }, { 
  projection: ['id', 'name', 'email'] 
});
```

#### `batchGet(keys: KeyInput<Entity, Key>[], options?: GetOptions): Promise<{ items: Entity[] }>`

Batch get multiple items.

```typescript
const result = await users.batchGet([
  { id: 'user-1' },
  { id: 'user-2' },
]);
// result.items is Entity[]

// Batch get with consistent read
const result = await users.batchGet(
  [{ id: 'user-1' }, { id: 'user-2' }],
  { consistentRead: true }
);

// Batch get with projection
const result = await users.batchGet(
  [{ id: 'user-1' }, { id: 'user-2' }],
  { projection: ['id', 'name'] }
);
```

#### `put(item: Entity, options?: PutOptions): Promise<Entity>`

Put (create or replace) an item.

```typescript
const user = await users.put({
  id: crypto.randomUUID(),
  name: 'John Doe',
  email: 'john@example.com',
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
});

// Conditional put (only if doesn't exist)
await users.put(item, {
  condition: (c) => c.notExists('id'),
});
```

#### `update(key: KeyInput<Entity, Key>, updates: UpdateInput<Entity, Key>, options?: UpdateOptions): Promise<Entity>`

Update an item by its key.

```typescript
const updatedUser = await users.update(
  { id: 'user-123' },
  { name: 'Jane Doe', updatedAt: new Date().toISOString() }
);

// Conditional update
await users.update(
  { id: 'user-123' },
  { status: 'active' },
  {
    condition: (c) => c.eq('status', 'pending'),
  }
);

// Update with returnValues option (default: 'ALL_NEW')
const oldUser = await users.update(
  { id: 'user-123' },
  { name: 'New Name' },
  { returnValues: 'ALL_OLD' }
);
```

#### `delete(key: KeyInput<Entity, Key>, options?: DeleteOptions): Promise<void>`

Delete an item by its key.

```typescript
await users.delete({ id: 'user-123' });

// Conditional delete
await users.delete(
  { id: 'user-123' },
  {
    condition: (c) => c.exists('id'),
  }
);
```

#### `query(partitionKey: PartitionKeyValue<Entity, Key>, options?: QueryOptions<Entity>): Promise<Entity[]>`

Query items by partition key (and optionally sort key condition).

```typescript
// Query by partition key (single partition key table)
const foundUsers = await users.query({ id: 'user-123' });

// Query with filter
const filteredUsers = await users.query(
  { id: 'user-123' },
  {
    filter: (f) => f.gt('createdAt', '2025-01-01'),
  }
);

// Query with sort key condition (for tables with sort keys)
const orders = await ordersTable.query(
  { userId: 'user-123' },
  {
    sortKey: (sk) => sk.between('2025-01-01', '2025-12-31'),
    filter: (f) => f.eq('status', 'active'),
  }
);

// Query with pagination
const page1 = await users.query({ id: 'user-123' }, { limit: 10 });
const page2 = await users.query(
  { id: 'user-123' },
  { limit: 10, exclusiveStartKey: page1LastEvaluatedKey }
);
```

#### `queryIndex(indexName: string, partitionKey: PartitionKeyValue<Entity, IndexKey>, options?: QueryOptions<Entity>): Promise<Entity[]>`

Query a Global Secondary Index or Local Secondary Index.

```typescript
// Query by index
const users = await users.queryIndex('emailIndex', { email: 'john@example.com' });

// Query index with sort key condition and filter
const orders = await ordersTable.queryIndex(
  'userOrdersIndex',
  { userId: 'user-123' },
  {
    sortKey: (sk) => sk.between(startDate, endDate),
    filter: (f) => f.eq('status', 'active'),
  }
);
```

#### `scan(options?: ScanOptions): Promise<Entity[]>`

Scan the entire table.

```typescript
const allUsers = await users.scan();

// Scan with filter
const activeUsers = await users.scan({
  filter: (f) => f.eq('status', 'active'),
  limit: 100,
});
```

#### `paginate(options: QueryOptions | ScanOptions, paginateOptions?): AsyncGenerator<PaginateResult<Entity>, void, unknown>`

Paginate through query or scan results.

```typescript
// Paginate scan results
for await (const page of users.paginate(
  { filter: (f) => f.eq('status', 'active'), limit: 100 },
  { limit: 50 }
)) {
  // Process each page
  console.log(page.data); // Array of items
  console.log(page.hasNextPage); // boolean
  console.log(page.nextPageToken); // string | undefined
}
```

**Note:** Currently, `paginate` only supports scan operations. Query pagination support may be added in future versions.

#### `transaction(requests: TransactionRequest<Entity, Key>[]): Promise<{ items: Entity[] }>`

Execute a transaction with multiple requests (put, update, or delete).

```typescript
const result = await users.transaction([
  { 
    type: 'put', 
    item: { id: '1', name: 'User 1', email: 'user1@example.com', createdAt: '...', updatedAt: '...' },
    condition: (c) => c.notExists('id'), // optional
  },
  { 
    type: 'update', 
    key: { id: '2' }, 
    updates: { name: 'Updated User 2', updatedAt: '...' },
    condition: (c) => c.eq('status', 'active'), // optional
  },
  { 
    type: 'delete', 
    key: { id: '3' },
    condition: (c) => c.exists('id'), // optional
  },
]);
// result.items contains items from put and update operations
```

## Expression Builder Methods

The expression builder (used for both filters and conditions) supports the following methods:

### Comparison Operations
- `eq(attribute, value)` - Equality
- `ne(attribute, value)` - Inequality
- `lt(attribute, value)` - Less than
- `lte(attribute, value)` - Less than or equal
- `gt(attribute, value)` - Greater than
- `gte(attribute, value)` - Greater than or equal
- `beginsWith(attribute, value)` - Begins with (string)
- `contains(attribute, value)` - Contains (string or array)
- `between(attribute, start, end)` - Between
- `in(attribute, values[])` - In array

### Attribute Checks
- `exists(attribute)` - Attribute exists
- `notExists(attribute)` - Attribute does not exist
- `attributeType(attribute, type)` - Attribute type check (condition only)

### Size Operations
- `size(attribute)` - Size of attribute
- `sizeGt(attribute, value)` - Size greater than
- `sizeLt(attribute, value)` - Size less than
- `sizeGte(attribute, value)` - Size greater than or equal (condition only)
- `sizeLte(attribute, value)` - Size less than or equal (condition only)

### Logical Operations
- `and(...conditions)` - Logical AND
- `or(...conditions)` - Logical OR
- `not(condition)` - Logical NOT

### Sort Key Condition Builder

When querying tables or indexes with sort keys, use the `sortKey` option with a `SortKeyConditionBuilder`:

```typescript
sortKey: (sk) => sk.eq('value')
sortKey: (sk) => sk.between('start', 'end')
sortKey: (sk) => sk.beginsWith('prefix')
sortKey: (sk) => sk.gt('value')
sortKey: (sk) => sk.gte('value')
sortKey: (sk) => sk.lt('value')
sortKey: (sk) => sk.lte('value')
```

**Note:** Sort key conditions support only: `=`, `<`, `<=`, `>`, `>=`, `BETWEEN`, and `begins_with`.

## Custom Methods

Add custom repository methods to your table instance using method factories:

```typescript
const users = table(client, {
  tableName: 'users',
  key: { pk: 'id' },
  schema: UserSchema,
  methods: {
    getActiveUsers: (methods) => {
      return async () => {
        return methods.scan({
          filter: (f) => f.eq('status', 'active'),
        });
      };
    },
    getUserByEmail: (methods) => {
      return async (email: string) => {
        const users = await methods.queryIndex('emailIndex', { email });
        return users[0] ?? null;
      };
    },
  },
});

// Use custom methods
const activeUsers = await users.getActiveUsers();
const user = await users.getUserByEmail('john@example.com');
```

**Note:** Method factories receive the table methods object as their first parameter, allowing you to call other table methods within your custom methods.

## Type Safety

`itty-repo` provides full type inference from your Valibot schemas (conforming to StandardSchemaV1). All methods are fully typed, and TypeScript type information is preserved, including:

- **Branded types** (e.g., `string & { __brand: 'UUID' }`)
- **Literal types** (e.g., `'pending' | 'active' | 'inactive'`)
- **Template literal types**
- **Union types, intersection types, and all other TypeScript types**
- **Nested attribute paths** (e.g., `'address.city'`, `'items[0].name'`)

### Attribute Paths

The expression builder supports nested attribute paths for complex data structures:

```typescript
// Nested object paths
filter: (f) => f.eq('address.city', 'New York')

// Array element paths
filter: (f) => f.eq('permissions[0]', 'admin')

// Nested paths within arrays
filter: (f) => f.eq('items[0].name', 'Product Name')

// Array object property paths
filter: (f) => f.contains('metadata.key', 'test')
```

## License

MIT
