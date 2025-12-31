import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  QueryCommand,
  ScanCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
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
  client: DynamoDBClient;
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
      new GetItemCommand({
        TableName: ctx.tableName,
        Key: marshall(keyOf(key)),
        ...options,
      })
    );
    return { item: Item ? (unmarshall(Item) as Entity) : null };
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
    const { ExpressionAttributeValues, ...restOptions } = options ?? {};
    const { Attributes } = await ctx.client.send(
      new PutItemCommand({
        TableName: ctx.tableName,
        Item: marshall(item as Record<string, unknown>),
        ...restOptions,
        ExpressionAttributeValues: ExpressionAttributeValues
          ? marshall(ExpressionAttributeValues)
          : undefined,
      })
    );
    // Return the item or the returned attributes if ReturnValues was specified
    return { item: Attributes ? (unmarshall(Attributes) as Entity) : item };
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

    const mergedExpressionAttributeValues = mergeAndNormalize(
      expr.ExpressionAttributeValues,
      options?.ExpressionAttributeValues
    );

    const { Attributes } = await ctx.client.send(
      new UpdateItemCommand({
        TableName: ctx.tableName,
        Key: marshall(keyOf(key)),
        UpdateExpression: expr.UpdateExpression,
        ExpressionAttributeNames: mergeAndNormalize(
          expr.ExpressionAttributeNames,
          options?.ExpressionAttributeNames
        ) as Record<string, string> | undefined,
        ExpressionAttributeValues: mergedExpressionAttributeValues
          ? marshall(mergedExpressionAttributeValues)
          : undefined,
        ReturnValues: options?.ReturnValues ?? 'ALL_NEW',
        ConditionExpression: options?.ConditionExpression,
        ReturnConsumedCapacity: options?.ReturnConsumedCapacity,
        ReturnItemCollectionMetrics: options?.ReturnItemCollectionMetrics,
      })
    );

    if (!Attributes && (options?.ReturnValues === 'ALL_NEW' || !options?.ReturnValues)) {
      throw new Error('Update operation completed but no attributes were returned');
    }

    return { item: Attributes ? (unmarshall(Attributes) as Entity) : ({} as Entity) };
  };
};

// ============================================================================
// Remove Operation
// ============================================================================

const remove = <Entity, Key extends KeyConfig>(ctx: OperationContext<Key>) => {
  const keyOf = createKeyOf<Entity, Key>(ctx.keyConfig);

  return async (key: KeyInput<Entity, Key>, options?: DeleteOptions): Promise<DeleteResult> => {
    const { ExpressionAttributeValues, ...restOptions } = options ?? {};
    await ctx.client.send(
      new DeleteItemCommand({
        TableName: ctx.tableName,
        Key: marshall(keyOf(key)),
        ...restOptions,
        ExpressionAttributeValues: ExpressionAttributeValues
          ? marshall(ExpressionAttributeValues)
          : undefined,
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
    const { pk, skCondition, indexName, exclusiveStartKey, ExpressionAttributeValues, ...rest } =
      options;

    const keyCond = buildKeyConditionExpression(
      ctx.keyConfig.pk,
      pk,
      ctx.keyConfig.sk,
      skCondition
    );

    const mergedExpressionAttributeValues = mergeAndNormalize(
      keyCond.ExpressionAttributeValues,
      ExpressionAttributeValues
    );

    const result = await ctx.client.send(
      new QueryCommand({
        TableName: ctx.tableName,
        IndexName: indexName,
        ...rest,
        KeyConditionExpression: keyCond.KeyConditionExpression,
        ExpressionAttributeNames: mergeAndNormalize(
          keyCond.ExpressionAttributeNames,
          rest.ExpressionAttributeNames
        ) as Record<string, string> | undefined,
        ExpressionAttributeValues: mergedExpressionAttributeValues
          ? marshall(mergedExpressionAttributeValues)
          : undefined,
        ExclusiveStartKey: exclusiveStartKey ? marshall(exclusiveStartKey) : undefined,
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
    const { exclusiveStartKey, ExpressionAttributeValues, ...rest } = options ?? {};
    const result = await ctx.client.send(
      new ScanCommand({
        TableName: ctx.tableName,
        ...rest,
        ExclusiveStartKey: exclusiveStartKey ? marshall(exclusiveStartKey) : undefined,
        ExpressionAttributeValues: ExpressionAttributeValues
          ? marshall(ExpressionAttributeValues)
          : undefined,
      })
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
    const transactItems = mapTransactItems(ctx.tableName, requests).map((item) => {
      if ('Put' in item) {
        return {
          Put: {
            ...item.Put,
            Item: marshall(item.Put.Item),
          },
        };
      }
      if ('Update' in item) {
        return {
          Update: {
            ...item.Update,
            Key: marshall(item.Update.Key),
            ExpressionAttributeValues: item.Update.ExpressionAttributeValues
              ? marshall(item.Update.ExpressionAttributeValues)
              : undefined,
          },
        };
      }
      return {
        Delete: {
          ...item.Delete,
          Key: marshall(item.Delete.Key),
        },
      };
    });

    await ctx.client.send(
      new TransactWriteItemsCommand({
        TransactItems: transactItems,
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
  return Object.fromEntries(
    Object.entries(config.schemas).map(([entityName, entityConfig]) => {
      type Entity = InferEntityOutput<typeof entityConfig.schema>;
      type Key = typeof entityConfig.key;

      const ctx: OperationContext<Key> = {
        client,
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
