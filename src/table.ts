import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import {
  GetCommand,
  PutCommand,
  UpdateCommand,
  DeleteCommand,
  QueryCommand,
  ScanCommand,
  TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';
import type { StandardSchemaV1 } from '@standard-schema/spec';
import type {
  TableConfig,
  Table,
  EntityConfig,
  InferEntityOutput,
  KeyConfig,
  KeyInput,
  GetResult,
  PutResult,
  UpdateInput,
  UpdateResult,
  DeleteResult,
  QueryOptions,
  ScanOptions,
  PagedResult,
  PaginateOptions,
  TransactWriteRequest,
  GetOptions,
  PutOptions,
  UpdateOptions,
  DeleteOptions,
} from './types.js';
import {
  extractAndValidateKey,
  validateKeyInput,
  buildUpdateExpression,
  buildKeyConditionExpression,
  mergeAndNormalize,
  toPagedResult,
  decodePageKey,
  mapTransactItems,
} from './utils.js';

// ============================================================================
// Shared Context
// ============================================================================

interface OperationContext<Key extends KeyConfig> {
  client: DynamoDBDocumentClient;
  tableName: string;
  keyConfig: Key;
}

// ============================================================================
// Key Helper Factory
// ============================================================================

const createKeyOf =
  <Entity, Key extends KeyConfig>(keyConfig: Key) =>
  (key: KeyInput<Entity, Key>) =>
    extractAndValidateKey(key, keyConfig);

// ============================================================================
// Get Operation
// ============================================================================

const get = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  const keyOf = createKeyOf<Entity, Key>(ctx.keyConfig);

  return async (key: KeyInput<Entity, Key>, options?: GetOptions): Promise<GetResult<Entity>> => {
    const { Item } = await ctx.client.send(
      new GetCommand({
        TableName: ctx.tableName,
        Key: keyOf(key),
        ...options,
      })
    );
    return { item: (Item as Entity) ?? null };
  };
};

// ============================================================================
// Put Operation
// ============================================================================

const put = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  const validateKey = (item: Entity) =>
    validateKeyInput(item as KeyInput<Entity, Key>, ctx.keyConfig);

  return async (item: Entity, options?: PutOptions): Promise<PutResult<Entity>> => {
    validateKey(item);
    const { Attributes } = await ctx.client.send(
      new PutCommand({
        TableName: ctx.tableName,
        Item: item as Record<string, unknown>,
        ...options,
      })
    );
    // Return the item or the returned attributes if ReturnValues was specified
    return { item: (Attributes as Entity) ?? item };
  };
};

// ============================================================================
// Update Operation
// ============================================================================

const update = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  const keyOf = createKeyOf<Entity, Key>(ctx.keyConfig);

  return async (
    key: KeyInput<Entity, Key>,
    updates: UpdateInput<Entity, Key>,
    options?: UpdateOptions
  ): Promise<UpdateResult<Entity>> => {
    if (Object.keys(updates).length === 0) {
      throw new Error('Updates object cannot be empty');
    }

    const expr = buildUpdateExpression(updates);

    if (!expr.UpdateExpression) {
      throw new Error('UpdateExpression is required but was not generated');
    }

    const { Attributes } = await ctx.client.send(
      new UpdateCommand({
        TableName: ctx.tableName,
        Key: keyOf(key),
        UpdateExpression: expr.UpdateExpression,
        ExpressionAttributeNames: mergeAndNormalize(
          expr.ExpressionAttributeNames,
          options?.ExpressionAttributeNames
        ) as Record<string, string> | undefined,
        ExpressionAttributeValues: mergeAndNormalize(
          expr.ExpressionAttributeValues,
          options?.ExpressionAttributeValues
        ),
        ReturnValues: options?.ReturnValues ?? 'ALL_NEW',
        ConditionExpression: options?.ConditionExpression,
        ReturnConsumedCapacity: options?.ReturnConsumedCapacity,
        ReturnItemCollectionMetrics: options?.ReturnItemCollectionMetrics,
      })
    );

    if (!Attributes && (options?.ReturnValues === 'ALL_NEW' || !options?.ReturnValues)) {
      throw new Error('Update operation completed but no attributes were returned');
    }

    return { item: (Attributes as Entity) ?? ({} as Entity) };
  };
};

// ============================================================================
// Remove Operation
// ============================================================================

const remove = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  const keyOf = createKeyOf<Entity, Key>(ctx.keyConfig);

  return async (key: KeyInput<Entity, Key>, options?: DeleteOptions): Promise<DeleteResult> => {
    await ctx.client.send(
      new DeleteCommand({
        TableName: ctx.tableName,
        Key: keyOf(key),
        ...options,
      })
    );
    return { success: true };
  };
};

// ============================================================================
// Query Operation
// ============================================================================

const query = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  return async (options: QueryOptions<Entity>): Promise<PagedResult<Entity>> => {
    const { pk, skCondition, indexName, ...rest } = options;

    const keyCond = buildKeyConditionExpression(
      ctx.keyConfig.pk,
      pk,
      ctx.keyConfig.sk,
      skCondition
    );

    const result = await ctx.client.send(
      new QueryCommand({
        TableName: ctx.tableName,
        IndexName: indexName,
        ...rest,
        ...keyCond,
        ExpressionAttributeNames: mergeAndNormalize(
          keyCond.ExpressionAttributeNames,
          rest.ExpressionAttributeNames
        ) as Record<string, string> | undefined,
        ExpressionAttributeValues: mergeAndNormalize(
          keyCond.ExpressionAttributeValues,
          rest.ExpressionAttributeValues
        ),
      })
    );

    return toPagedResult(result);
  };
};

// ============================================================================
// Scan Operation
// ============================================================================

const scan = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  return async (options?: ScanOptions): Promise<PagedResult<Entity>> => {
    const result = await ctx.client.send(
      new ScanCommand({ TableName: ctx.tableName, ...(options ?? {}) })
    );
    return toPagedResult(result);
  };
};

// ============================================================================
// Paginate Operation
// ============================================================================

const paginate = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  const queryFn = query<Entity, Key>(ctx);
  const scanFn = scan<Entity, Key>(ctx);

  return async function* (
    options: QueryOptions<Entity> | ScanOptions,
    paginateOptions?: PaginateOptions<Entity>
  ): AsyncGenerator<Entity[], void, unknown> {
    const { maxPages = Infinity, onPage } = paginateOptions ?? {};

    let page = 0;
    let exclusiveStartKey: Record<string, unknown> | undefined = options.exclusiveStartKey;

    const fetchPage = (esk?: Record<string, unknown>) => {
      const current = { ...options, exclusiveStartKey: esk };
      return 'pk' in current
        ? queryFn(current as QueryOptions<Entity>)
        : scanFn(current as ScanOptions);
    };

    while (page < maxPages) {
      const result = await fetchPage(exclusiveStartKey);

      if (onPage) await onPage(result.items);
      yield result.items;

      if (!result.hasNextPage) return;

      exclusiveStartKey = result.nextPageKey ? decodePageKey(result.nextPageKey) : undefined;

      page++;
    }
  };
};

// ============================================================================
// Transaction Operation
// ============================================================================

const transaction = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  return async (requests: TransactWriteRequest<Entity>[]): Promise<void> => {
    await ctx.client.send(
      new TransactWriteCommand({
        TransactItems: mapTransactItems(ctx.tableName, requests),
      })
    );
  };
};

// ============================================================================
// Table Factory
// ============================================================================

export const table = <Schemas extends Record<string, EntityConfig<StandardSchemaV1>>>(
  client: DynamoDBClient,
  config: TableConfig<Schemas>
): Table<Schemas> => {
  const docClient = DynamoDBDocumentClient.from(client);

  return Object.fromEntries(
    Object.entries(config.schemas).map(([entityName, entityConfig]) => {
      type Entity = InferEntityOutput<typeof entityConfig.schema>;
      type Key = typeof entityConfig.key;

      const ctx: OperationContext<Key> = {
        client: docClient,
        tableName: config.tableName,
        keyConfig: entityConfig.key,
      };

      const methods = {
        get: get<Entity, Key>(ctx),
        put: put<Entity, Key>(ctx),
        update: update<Entity, Key>(ctx),
        delete: remove<Entity, Key>(ctx),
        query: query<Entity, Key>(ctx),
        scan: scan<Entity, Key>(ctx),
        paginate: paginate<Entity, Key>(ctx),
        transaction: transaction<Entity, Key>(ctx),
      };

      return [entityName, methods];
    })
  ) as Table<Schemas>;
};
