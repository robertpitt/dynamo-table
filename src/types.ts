import type { StandardSchemaV1 } from '@standard-schema/spec';
import type {
  GetCommandInput,
  PutCommandInput,
  UpdateCommandInput,
  DeleteCommandInput,
  QueryCommandInput,
  ScanCommandInput,
  BatchGetCommandInput,
  BatchWriteCommandInput,
  TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';

/**
 * Key structure configuration for an entity.
 * Defines which fields in the entity schema are used as partition key (pk) and sort key (sk).
 */
export interface KeyConfig {
  /** The field name that serves as the partition key */
  pk: string;
  /** The field name that serves as the sort key (optional) */
  sk?: string;
}

/**
 * Configuration for a single entity in a table.
 */
export interface EntityConfig<Schema extends StandardSchemaV1 = StandardSchemaV1> {
  /** The key structure for this entity */
  key: KeyConfig;
  /** The Standard Schema V1 schema for this entity */
  schema: Schema;
}

/**
 * Configuration for a DynamoDB table with multiple entities.
 */
export interface TableConfig<
  Schemas extends Record<string, EntityConfig> = Record<string, EntityConfig>,
> {
  /** The DynamoDB table name */
  tableName: string;
  /** Map of entity names to their configurations */
  schemas: Schemas;
}

/**
 * Extracts the output type from a Standard Schema.
 */
export type InferEntityOutput<Schema extends StandardSchemaV1> =
  StandardSchemaV1.InferOutput<Schema>;

/**
 * Extracts the input type from a Standard Schema.
 */
export type InferEntityInput<Schema extends StandardSchemaV1> = StandardSchemaV1.InferInput<Schema>;

/**
 * Helper type to extract a key field from Entity if it exists
 */
type KeyField<Entity, KeyName extends string> = KeyName extends keyof Entity ? KeyName : never;

/**
 * Key input for operations that require a key.
 * This is a record with the key field names as properties.
 * The values must match the types of those fields in the Entity.
 */
export type KeyInput<Entity, Key extends KeyConfig> = Key['sk'] extends string
  ? Pick<Entity, KeyField<Entity, Key['pk']> | KeyField<Entity, Key['sk']>>
  : Pick<Entity, KeyField<Entity, Key['pk']>>;

/**
 * Input for put operations - uses the input type from Standard Schema.
 */
export type PutInput<Schema extends StandardSchemaV1> = InferEntityInput<Schema>;

/**
 * Input for update operations - partial entity with fields to update.
 * Key fields are excluded to prevent accidental key updates.
 */
export type UpdateInput<Entity, Key extends KeyConfig = KeyConfig> = Omit<
  Partial<Entity>,
  Key['pk'] | (Key['sk'] extends string ? Key['sk'] : never)
>;

/**
 * Options for projection expressions (selecting specific attributes).
 * Used by Get, Query, and Scan operations.
 */
export interface ProjectionOptions {
  /** Projection expression to specify which attributes to retrieve */
  ProjectionExpression?: string;
  /** Expression attribute names for projection expression */
  ExpressionAttributeNames?: Record<string, string>;
}

/**
 * Options for conditional operations (conditional writes).
 * Used by Put, Update, and Delete operations.
 */
export interface ConditionOptions {
  /** Condition expression that must be true for the operation to succeed */
  ConditionExpression?: string;
  /** Expression attribute names for condition expression */
  ExpressionAttributeNames?: Record<string, string>;
  /** Expression attribute values for condition expression */
  ExpressionAttributeValues?: Record<string, unknown>;
}

/**
 * Options for return values and metrics.
 * Used by Put, Update, and Delete operations.
 */
export interface ReturnAttributeOptions {
  /** What values to return after the write operation */
  ReturnValues?: 'NONE' | 'ALL_OLD' | 'UPDATED_OLD' | 'ALL_NEW' | 'UPDATED_NEW';
  /** Return consumed capacity information */
  ReturnConsumedCapacity?: 'INDEXES' | 'TOTAL' | 'NONE';
  /** Return item collection metrics */
  ReturnItemCollectionMetrics?: 'SIZE' | 'NONE';
}

/**
 * Options for consistent read behavior.
 * Used by Get and Query operations.
 */
export interface ConsistentReadOptions {
  /** Whether to use consistent read */
  ConsistentRead?: boolean;
}

/**
 * Options for consumed capacity reporting.
 * Can be used by any operation that supports it.
 */
export interface ConsumedCapacityOptions {
  /** Return consumed capacity information */
  ReturnConsumedCapacity?: 'INDEXES' | 'TOTAL' | 'NONE';
}

/**
 * Result of a get operation.
 */
export interface GetResult<Entity> {
  /** The retrieved item, or null if not found */
  item: Entity | null;
}

/**
 * Result of a put operation.
 */
export interface PutResult<Entity> {
  /** The item that was put */
  item: Entity;
}

/**
 * Result of an update operation.
 */
export interface UpdateResult<Entity> {
  /** The updated item */
  item: Entity;
}

/**
 * Result of a delete operation.
 */
export interface DeleteResult {
  /** Whether the deletion was successful */
  success: boolean;
}

/**
 * Paged result for query and scan operations.
 */
export interface PagedResult<Entity> {
  /** Whether there are more pages available */
  hasNextPage: boolean;
  /** The key for the next page (base64 encoded) */
  nextPageKey?: string;
  /** The items in this page */
  items: Entity[];
}

/**
 * Options for Get operations.
 */
export interface GetOptions
  extends ProjectionOptions, ConsistentReadOptions, ConsumedCapacityOptions {}

/**
 * Options for Put operations.
 */
export interface PutOptions extends ConditionOptions, ReturnAttributeOptions {}

/**
 * Options for Update operations.
 */
export interface UpdateOptions extends ConditionOptions, ReturnAttributeOptions {}

/**
 * Options for Delete operations.
 */
export interface DeleteOptions extends ConditionOptions, ReturnAttributeOptions {}

/**
 * Query options extending DynamoDB QueryCommandInput.
 */
export interface QueryOptions<Entity> extends Omit<
  QueryCommandInput,
  'TableName' | 'KeyConditionExpression'
> {
  /** The partition key value */
  pk: string;
  /** Optional sort key condition */
  skCondition?: string;
  /** Optional index name for GSI queries */
  indexName?: string;
  /** Optional filter expression */
  filterExpression?: string;
  /** Optional limit */
  limit?: number;
  /** Whether to scan index forward */
  scanIndexForward?: boolean;
  /** Exclusive start key for pagination */
  exclusiveStartKey?: Record<string, unknown>;
}

/**
 * Scan options extending DynamoDB ScanCommandInput.
 */
export interface ScanOptions extends Omit<ScanCommandInput, 'TableName'> {
  /** Optional filter expression */
  filterExpression?: string;
  /** Optional limit */
  limit?: number;
  /** Exclusive start key for pagination */
  exclusiveStartKey?: Record<string, unknown>;
}

/**
 * Paginate options for query or scan operations.
 */
export interface PaginateOptions<Entity = unknown> {
  /** Maximum number of pages to fetch */
  maxPages?: number;
  /** Callback for each page */
  onPage?: (items: Entity[]) => void | Promise<void>;
}

/**
 * Batch get options.
 */
export interface BatchGetOptions extends Omit<BatchGetCommandInput, 'RequestItems'> {}

/**
 * Batch write request - either a put or delete operation.
 */
export type BatchWriteRequest<Entity> =
  | { type: 'put'; item: Entity }
  | { type: 'delete'; key: Record<string, unknown> };

/**
 * Batch write options.
 */
export interface BatchWriteOptions extends Omit<BatchWriteCommandInput, 'RequestItems'> {}

/**
 * Transaction write request - put, update, or delete operation.
 */
export type TransactWriteRequest<Entity> =
  | { type: 'put'; item: Entity; conditionExpression?: string }
  | {
      type: 'update';
      key: Record<string, unknown>;
      updateExpression: string;
      expressionAttributeNames?: Record<string, string>;
      expressionAttributeValues?: Record<string, unknown>;
      conditionExpression?: string;
    }
  | { type: 'delete'; key: Record<string, unknown>; conditionExpression?: string };

/**
 * Transaction write options.
 */
export interface TransactWriteOptions extends Omit<TransactWriteCommandInput, 'TransactItems'> {}

/**
 * Entity methods interface - the methods available on each entity.
 */
export interface EntityMethods<Entity, Key extends KeyConfig> {
  /**
   * Get an item by its key.
   */
  get(key: KeyInput<Entity, Key>, options?: GetOptions): Promise<GetResult<Entity>>;

  /**
   * Put (create or replace) an item.
   */
  put(item: Entity, options?: PutOptions): Promise<PutResult<Entity>>;

  /**
   * Update an item by its key.
   */
  update(
    key: KeyInput<Entity, Key>,
    updates: UpdateInput<Entity, Key>,
    options?: UpdateOptions
  ): Promise<UpdateResult<Entity>>;

  /**
   * Delete an item by its key.
   */
  delete(key: KeyInput<Entity, Key>, options?: DeleteOptions): Promise<DeleteResult>;

  /**
   * Query items by partition key (and optional sort key condition).
   */
  query(options: QueryOptions<Entity>): Promise<PagedResult<Entity>>;

  /**
   * Scan the table.
   */
  scan(options?: ScanOptions): Promise<PagedResult<Entity>>;

  /**
   * Paginate through query or scan results.
   */
  paginate(
    options: QueryOptions<Entity> | ScanOptions,
    paginateOptions?: PaginateOptions<Entity>
  ): AsyncGenerator<Entity[], void, unknown>;

  /**
   * Transaction write multiple items.
   */
  transaction(requests: TransactWriteRequest<Entity>[]): Promise<void>;
}

/**
 * Table type - maps entity names to their methods.
 */
export type Table<Schemas extends Record<string, EntityConfig> = Record<string, EntityConfig>> = {
  [K in keyof Schemas]: Schemas[K] extends EntityConfig<infer Schema>
    ? EntityMethods<InferEntityOutput<Schema>, Schemas[K]['key']>
    : never;
};
