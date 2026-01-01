import { StandardSchemaV1 } from '@standard-schema/spec';
import type { ExpressionBuilder, SortKeyConditionBuilder } from './expression.js';

/**
 * Extracts required (non-optional) keys from a type
 * @internal
 */
type RequiredKeys<T> = {
  [K in keyof T]-?: {} extends Pick<T, K> ? never : K;
}[keyof T];

/**
 * Extracts a key field name from entity, ensuring it exists
 * @internal
 */
type KeyField<TEntity, TKeyName extends string> = TKeyName extends keyof TEntity ? TKeyName : never;

/**
 * Base key configuration without entity constraints.
 * Used internally for both table primary keys and index keys.
 */
export interface KeyConfigBase {
  /** Partition key field name */
  pk: string;
  /** Sort key field name (optional) */
  sk?: string;
}

/**
 * Key configuration with entity type constraints.
 * Ensures pk and sk are required (non-optional) fields from the entity type.
 * Used for both table primary keys and index keys.
 */
export interface KeyConfig<TEntity = unknown> extends Omit<KeyConfigBase, 'pk' | 'sk'> {
  /** Partition key field name - must be a required field in the entity */
  pk: RequiredKeys<TEntity>;
  /** Sort key field name (optional) - must be a required field in the entity if provided */
  sk?: RequiredKeys<TEntity>;
}

/**
 * Extracts the input type from a StandardSchemaV1 schema
 */
export type InferInput<TSchema extends StandardSchemaV1> = StandardSchemaV1.InferInput<TSchema>;

/**
 * Extracts the output type from a StandardSchemaV1 schema
 */
export type InferOutput<TSchema extends StandardSchemaV1> = StandardSchemaV1.InferOutput<TSchema>;

/**
 * Creates a key input type from entity and key configuration.
 * Extracts the pk field (required) and sk field (optional if defined in config).
 * Preserves literal types from the key config.
 */
export type KeyInput<TEntity, TKeyConfig extends KeyConfigBase> = TKeyConfig['sk'] extends string
  ? Pick<TEntity, KeyField<TEntity, TKeyConfig['pk']> | KeyField<TEntity, TKeyConfig['sk']>>
  : Pick<TEntity, KeyField<TEntity, TKeyConfig['pk']>>;

/**
 * Extracts partition key value type (just the partition key field).
 * Used for query operations where only the partition key is required.
 */
export type PartitionKeyValue<TEntity, TKeyConfig extends KeyConfigBase> = 
  Pick<TEntity, KeyField<TEntity, TKeyConfig['pk']>>;

/**
 * Creates an index key input type from entity and index key configuration.
 * Extracts the pk field (required) and sk field (optional if defined in config).
 * Uses the same pattern as KeyInput for consistency.
 */
export type IndexKeyInput<TEntity, TIndexKeyConfig extends KeyConfigBase> = TIndexKeyConfig['sk'] extends string
  ? Pick<TEntity, KeyField<TEntity, TIndexKeyConfig['pk']> | KeyField<TEntity, TIndexKeyConfig['sk']>>
  : Pick<TEntity, KeyField<TEntity, TIndexKeyConfig['pk']>>;

/**
 * Creates an update input type - partial entity excluding key fields.
 * Prevents accidental modification of key fields during update operations.
 */
export type UpdateInput<TEntity, TKeyConfig extends KeyConfigBase> = Partial<
  Omit<TEntity, TKeyConfig['pk'] | (TKeyConfig['sk'] extends string ? TKeyConfig['sk'] : never)>
>;

/**
 * Options for get operation
 */
export interface GetOptions {
  /** Use strongly consistent read (default: false) */
  consistentRead?: boolean;
  /** Project specific attributes to return */
  projection?: string[];
}

/**
 * Options for put operation
 */
export interface PutOptions<TEntity> {
  /** Condition expression that must be satisfied for the put to succeed */
  condition?: (c: ExpressionBuilder<TEntity>) => string;
}

/**
 * Options for update operation
 */
export interface UpdateOptions<TEntity> {
  /** Condition expression that must be satisfied for the update to succeed */
  condition?: (c: ExpressionBuilder<TEntity>) => string;
  /** What to return after update (default: 'ALL_NEW') */
  returnValues?: 'NONE' | 'ALL_OLD' | 'ALL_NEW' | 'UPDATED_OLD' | 'UPDATED_NEW';
}

/**
 * Options for delete operation
 */
export interface DeleteOptions<TEntity> {
  /** Condition expression that must be satisfied for the delete to succeed */
  condition?: (c: ExpressionBuilder<TEntity>) => string;
}

/**
 * Base query options (without sort key condition).
 * Used when querying tables/indexes without a sort key.
 * @internal
 */
interface QueryOptionsBase<TEntity> {
  /** Filter expression for non-key attributes */
  filter?: (f: ExpressionBuilder<TEntity>) => string;
  /** Maximum number of items to return */
  limit?: number;
  /** Exclusive start key for pagination (marshalled DynamoDB key) */
  exclusiveStartKey?: Record<string, any>;
  /** Reverse sort order (default: ascending) */
  scanIndexForward?: boolean;
}

/**
 * Query options with sort key condition.
 * Used when querying tables/indexes with a sort key.
 * @internal
 */
interface QueryOptionsWithSortKey<TEntity> extends QueryOptionsBase<TEntity> {
  /** Sort key condition builder */
  sortKey?: (sk: SortKeyConditionBuilder) => string;
}

/**
 * Options for query operation.
 * When used with a table/index that has a sort key, sortKey option is available.
 */
export type QueryOptions<TEntity, THasSortKey extends boolean = boolean> = THasSortKey extends true
  ? QueryOptionsWithSortKey<TEntity>
  : QueryOptionsBase<TEntity>;

/**
 * Options for scan operation
 */
export interface ScanOptions<TEntity> {
  /** Filter expression for non-key attributes */
  filter?: (f: ExpressionBuilder<TEntity>) => string;
  /** Maximum number of items to return */
  limit?: number;
  /** Exclusive start key for pagination (marshalled DynamoDB key) */
  exclusiveStartKey?: Record<string, any>;
}

/**
 * Options for paginate operation
 */
export interface PaginateOptions<TEntity> {
  /** Maximum number of items per page */
  limit?: number;
}

/**
 * Result type for batch get operation
 */
export interface BatchGetResult<TEntity> {
  /** Array of retrieved items */
  items: TEntity[];
}

/**
 * Result type for paginate operation
 */
export interface PaginateResult<TEntity> {
  /** Array of items in the current page */
  data: TEntity[];
  /** Whether there are more pages available */
  hasNextPage: boolean;
  /** Token for retrieving the next page (if available) */
  nextPageToken?: string;
}

/**
 * Transaction request for put operation
 */
export interface TransactionPutRequest<TEntity> {
  type: 'put';
  /** Item to put */
  item: TEntity;
  /** Condition expression that must be satisfied */
  condition?: (c: ExpressionBuilder<TEntity>) => string;
}

/**
 * Transaction request for update operation
 */
export interface TransactionUpdateRequest<TEntity, TKeyConfig extends KeyConfigBase> {
  type: 'update';
  /** Key of the item to update */
  key: KeyInput<TEntity, TKeyConfig>;
  /** Updates to apply */
  updates: UpdateInput<TEntity, TKeyConfig>;
  /** Condition expression that must be satisfied */
  condition?: (c: ExpressionBuilder<TEntity>) => string;
}

/**
 * Transaction request for delete operation
 */
export interface TransactionDeleteRequest<TEntity, TKeyConfig extends KeyConfigBase> {
  type: 'delete';
  /** Key of the item to delete */
  key: KeyInput<TEntity, TKeyConfig>;
  /** Condition expression that must be satisfied */
  condition?: (c: ExpressionBuilder<TEntity>) => string;
}

/**
 * Union type for all transaction request types
 */
export type TransactionRequest<TEntity, TKeyConfig extends KeyConfigBase> =
  | TransactionPutRequest<TEntity>
  | TransactionUpdateRequest<TEntity, TKeyConfig>
  | TransactionDeleteRequest<TEntity, TKeyConfig>;

/**
 * Index projection configuration for DynamoDB indexes
 */
export type IndexProjection = 'KEYS_ONLY' | 'ALL' | { include: string[] };

/**
 * Extracts index names as a string literal union type
 */
export type IndexNames<TIndexes> = TIndexes extends Record<string, any>
  ? keyof TIndexes & string
  : never;

/**
 * Extracts the key input type for a specific index
 */
export type IndexKeyInputForIndex<
  TEntity,
  TIndexes extends Record<string, { key: KeyConfigBase; projection: IndexProjection }>,
  TIndexName extends keyof TIndexes
> = TIndexes[TIndexName] extends { key: infer TIndexKey extends KeyConfigBase }
  ? IndexKeyInput<TEntity, TIndexKey>
  : never;

/**
 * Custom method factory type for extending table instances with custom methods
 * @internal
 */
export type MethodFactory = (methods: Record<string, unknown>) => unknown;

/**
 * Options for creating a table instance
 */
export interface TableOptions<TSchema extends StandardSchemaV1> {
  /** DynamoDB table name */
  tableName: string;
  /** Schema definition conforming to StandardSchemaV1 */
  schema: TSchema;
  /** Primary key configuration - pk and sk must be non-optional fields in the schema */
  key: KeyConfig<InferOutput<TSchema>>;
  /** Optional TTL field name */
  ttl?: string;
  /** Optional index definitions */
  indexes?: Record<
    string,
    {
      key: KeyConfig<InferOutput<TSchema>>;
      projection: IndexProjection;
    }
  >;
  /** Optional custom method factories */
  methods?: Record<string, MethodFactory>;
}

/**
 * Extended table options with specific key and index types.
 * Used to preserve literal types from key config and indexes when creating tables.
 */
export interface ExtendedTableOptions<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
> extends Omit<TableOptions<TSchema>, 'key' | 'indexes'> {
  key: TKeyConfig;
  indexes?: TIndexes;
}

/**
 * Table instance type that captures all type state from options.
 * Provides type-safe methods for all DynamoDB operations.
 * 
 * @template TOptions - Table options extending TableOptions<StandardSchemaV1>
 */
export type Table<TOptions extends TableOptions<StandardSchemaV1>> = {
  /**
   * Get a single item by key
   * @returns The item if found, undefined otherwise
   */
  get: (
    key: KeyInput<InferOutput<TOptions['schema']>, TOptions['key']>,
    options?: GetOptions
  ) => Promise<InferOutput<TOptions['schema']> | undefined>;

  /**
   * Put (create or replace) an item
   * @returns The item that was put
   */
  put: (
    item: InferInput<TOptions['schema']>,
    options?: PutOptions<InferInput<TOptions['schema']>>
  ) => Promise<InferOutput<TOptions['schema']>>;

  /**
   * Update an existing item
   * @returns The updated item
   */
  update: (
    key: KeyInput<InferOutput<TOptions['schema']>, TOptions['key']>,
    updates: UpdateInput<InferOutput<TOptions['schema']>, TOptions['key']>,
    options?: UpdateOptions<InferOutput<TOptions['schema']>>
  ) => Promise<InferOutput<TOptions['schema']>>;

  /**
   * Delete an item by key
   */
  delete: (
    key: KeyInput<InferOutput<TOptions['schema']>, TOptions['key']>,
    options?: DeleteOptions<InferOutput<TOptions['schema']>>
  ) => Promise<void>;

  /**
   * Query items by partition key (and optionally sort key condition).
   * When the table has a sort key, sortKey option is available.
   */
  query: TOptions['key']['sk'] extends string
    ? (
        partitionKey: PartitionKeyValue<InferOutput<TOptions['schema']>, TOptions['key']>,
        options?: QueryOptions<InferOutput<TOptions['schema']>, true>
      ) => Promise<InferOutput<TOptions['schema']>[]>
    : (
        partitionKey: PartitionKeyValue<InferOutput<TOptions['schema']>, TOptions['key']>,
        options?: QueryOptions<InferOutput<TOptions['schema']>, false>
      ) => Promise<InferOutput<TOptions['schema']>[]>;

  /**
   * Query items by index.
   * When the index has a sort key, sortKey option is available.
   */
  queryIndex: <TIndexName extends IndexNames<TOptions['indexes']>>(
    indexName: TIndexName,
    partitionKey: PartitionKeyValue<
      InferOutput<TOptions['schema']>,
      NonNullable<TOptions['indexes']>[TIndexName]['key']
    >,
    options?: NonNullable<TOptions['indexes']>[TIndexName]['key']['sk'] extends string
      ? QueryOptions<InferOutput<TOptions['schema']>, true>
      : QueryOptions<InferOutput<TOptions['schema']>, false>
  ) => Promise<InferOutput<TOptions['schema']>[]>;

  /**
   * Scan all items in the table
   * @returns Array of items matching the filter (if provided)
   */
  scan: (
    options?: ScanOptions<InferOutput<TOptions['schema']>>
  ) => Promise<InferOutput<TOptions['schema']>[]>;

  /**
   * Batch get multiple items by keys
   * @returns Result containing array of retrieved items
   */
  batchGet: (
    keys: KeyInput<InferOutput<TOptions['schema']>, TOptions['key']>[],
    options?: GetOptions
  ) => Promise<BatchGetResult<InferOutput<TOptions['schema']>>>;

  /**
   * Paginate through query or scan results
   * @returns Async generator yielding paginated results
   */
  paginate: (
    options: QueryOptions<InferOutput<TOptions['schema']>> | ScanOptions<InferOutput<TOptions['schema']>>,
    paginateOptions?: PaginateOptions<InferOutput<TOptions['schema']>>
  ) => AsyncGenerator<PaginateResult<InferOutput<TOptions['schema']>>, void, unknown>;

  /**
   * Execute a transaction with multiple requests
   * @returns Result containing array of items from put/update operations
   */
  transaction: (
    requests: TransactionRequest<InferInput<TOptions['schema']>, TOptions['key']>[]
  ) => Promise<{ items: InferOutput<TOptions['schema']>[] }>;
};
