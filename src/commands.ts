import {
  GetItemCommand,
  PutItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  QueryCommand,
  ScanCommand,
  BatchGetItemCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';
import type { StandardSchemaV1 } from '@standard-schema/spec';
import type {
  TableOptions,
  KeyConfig,
  InferOutput,
  InferInput,
  ExtendedTableOptions,
  KeyInput,
  PartitionKeyValue,
  UpdateInput,
  GetOptions,
  PutOptions,
  UpdateOptions,
  DeleteOptions,
  QueryOptions,
  ScanOptions,
  TransactionRequest,
} from './types.js';
import { createExpressionBuilder, createSortKeyConditionBuilder } from './expression.js';
import { buildKey, buildUpdateExpression, parseExclusiveStartKey } from './utils.js';

// Helper functions for DRY principle

/**
 * Builds a projection expression from attribute paths
 */
function buildProjectionExpression<TEntity>(
  projection: string[],
  builder: ReturnType<typeof createExpressionBuilder<TEntity>>
): string {
  const projectionParts = projection.map(attr => {
    const segments = attr.split('.');
    return segments.map(seg => {
      const key = `#${seg}`;
      if (!builder.names[key]) builder.names[key] = seg;
      return key;
    }).join('.');
  });
  return projectionParts.join(', ');
}

/**
 * Applies a condition expression to a command input (without merging existing attributes)
 */
function applyConditionExpression<TEntity>(
  condition: (c: ReturnType<typeof createExpressionBuilder<TEntity>>) => string,
  commandInput: any
): void {
  const builder = createExpressionBuilder<TEntity>();
  const conditionExpression = condition(builder);
  commandInput.ConditionExpression = conditionExpression;
  commandInput.ExpressionAttributeNames = builder.names;
  commandInput.ExpressionAttributeValues = marshall(builder.values);
}

/**
 * Applies a condition expression to a command input (merging with existing attributes)
 */
function applyConditionExpressionWithMerge<TEntity>(
  condition: (c: ReturnType<typeof createExpressionBuilder<TEntity>>) => string,
  commandInput: any
): void {
  const conditionBuilder = createExpressionBuilder<TEntity>();
  const conditionExpression = condition(conditionBuilder);
  commandInput.ConditionExpression = conditionExpression;
  
  // Merge expression attribute names
  commandInput.ExpressionAttributeNames = {
    ...commandInput.ExpressionAttributeNames,
    ...conditionBuilder.names,
  };
  
  // Merge expression attribute values
  const conditionValues = marshall(conditionBuilder.values);
  commandInput.ExpressionAttributeValues = {
    ...commandInput.ExpressionAttributeValues,
    ...conditionValues,
  };
}

/**
 * Applies a filter expression to a command input (without merging existing attributes)
 */
function applyFilterExpression<TEntity>(
  filter: (c: ReturnType<typeof createExpressionBuilder<TEntity>>) => string,
  commandInput: any
): void {
  const filterBuilder = createExpressionBuilder<TEntity>();
  const filterExpression = filter(filterBuilder);
  commandInput.FilterExpression = filterExpression;
  commandInput.ExpressionAttributeNames = filterBuilder.names;
  commandInput.ExpressionAttributeValues = marshall(filterBuilder.values);
}

/**
 * Applies a filter expression to a command input (merging with existing attributes)
 */
function applyFilterExpressionWithMerge<TEntity>(
  filter: (c: ReturnType<typeof createExpressionBuilder<TEntity>>) => string,
  commandInput: any
): void {
  const filterBuilder = createExpressionBuilder<TEntity>();
  const filterExpression = filter(filterBuilder);
  commandInput.FilterExpression = filterExpression;
  
  // Merge expression attribute names and values
  commandInput.ExpressionAttributeNames = {
    ...commandInput.ExpressionAttributeNames,
    ...filterBuilder.names,
  };
  const filterValues = marshall(filterBuilder.values);
  commandInput.ExpressionAttributeValues = {
    ...commandInput.ExpressionAttributeValues,
    ...filterValues,
  };
}

/**
 * Builds a key condition expression from partition key and optional sort key
 */
function buildKeyConditionExpression<TEntity>(
  partitionKey: any,
  keyConfig: { pk: string | number | symbol; sk?: string | number | symbol },
  options: any,
  builder: ReturnType<typeof createExpressionBuilder<TEntity>>
): string {
  const pkName = String(keyConfig.pk);
  const pkAttrName = `#${pkName}`;
  builder.names[pkAttrName] = pkName;
  const pkValueName = `:pk`;
  builder.values[pkValueName] = partitionKey[keyConfig.pk];
  let keyConditionExpression = `${pkAttrName} = ${pkValueName}`;

  // Build sort key condition if provided
  if (keyConfig.sk && options && 'sortKey' in options && options.sortKey) {
    const skBuilder = createSortKeyConditionBuilder(builder, String(keyConfig.sk) as any);
    const skCondition = options.sortKey(skBuilder);
    keyConditionExpression += ` AND ${skCondition}`;
  }

  return keyConditionExpression;
}

/**
 * Applies query options (limit, scanIndexForward, exclusiveStartKey) to a command input
 */
function applyQueryOptions(
  options: { limit?: number; scanIndexForward?: boolean; exclusiveStartKey?: string | Record<string, any> } | undefined,
  commandInput: any
): void {
  if (options?.limit) {
    commandInput.Limit = options.limit;
  }
  if (options?.scanIndexForward !== undefined) {
    commandInput.ScanIndexForward = options.scanIndexForward;
  }
  if (options?.exclusiveStartKey) {
    commandInput.ExclusiveStartKey = parseExclusiveStartKey(options.exclusiveStartKey);
  }
}

export function buildGetItemCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  // @ts-expect-error - KeyInput constraint issue with KeyConfig<TEntity>, works at runtime
  key: KeyInput<InferOutput<TSchema>, TKeyConfig>,
  options: GetOptions | undefined,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): GetItemCommand {
  const dynamoKey = buildKey(key, config.key as any);

  const commandInput: any = {
    TableName: config.tableName,
    Key: dynamoKey,
    ConsistentRead: options?.consistentRead ?? false,
  };

  // Handle projection
  if (options?.projection && options.projection.length > 0) {
    const builder = createExpressionBuilder<InferOutput<TSchema>>();
    commandInput.ProjectionExpression = buildProjectionExpression(options.projection, builder);
    commandInput.ExpressionAttributeNames = builder.names;
  }

  return new GetItemCommand(commandInput);
}

export function buildBatchGetItemCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  // @ts-expect-error - KeyInput constraint issue with KeyConfig<TEntity>, works at runtime
  keys: KeyInput<InferOutput<TSchema>, TKeyConfig>[],
  options: GetOptions | undefined,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): BatchGetItemCommand {
  const marshalledKeys = keys.map(key => buildKey(key, config.key as any));

  const requestItems: any = {
    [config.tableName]: {
      Keys: marshalledKeys,
      ConsistentRead: options?.consistentRead ?? false,
    },
  };

  // Handle projection
  if (options?.projection && options.projection.length > 0) {
    const builder = createExpressionBuilder<InferOutput<TSchema>>();
    requestItems[config.tableName].ProjectionExpression = buildProjectionExpression(options.projection, builder);
    requestItems[config.tableName].ExpressionAttributeNames = builder.names;
  }

  return new BatchGetItemCommand({ RequestItems: requestItems });
}

export function buildPutItemCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  item: InferInput<TSchema>,
  options: PutOptions<InferInput<TSchema>> | undefined,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): PutItemCommand {
  const marshalledItem = marshall(item as Record<string, any>);

  const commandInput: any = {
    TableName: config.tableName,
    Item: marshalledItem,
  };

  // Handle condition expression
  if (options?.condition) {
    applyConditionExpression<InferInput<TSchema>>(options.condition, commandInput);
  }

  return new PutItemCommand(commandInput);
}

export function buildUpdateItemCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  // @ts-expect-error - KeyInput constraint issue with KeyConfig<TEntity>, works at runtime
  key: KeyInput<InferOutput<TSchema>, TKeyConfig>,
  // @ts-expect-error - UpdateInput constraint issue with KeyConfig<TEntity>, works at runtime
  updates: UpdateInput<InferOutput<TSchema>, TKeyConfig>,
  options: UpdateOptions<InferOutput<TSchema>> | undefined,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): UpdateItemCommand {
  const dynamoKey = buildKey(key, config.key as any);
  const builder = createExpressionBuilder<InferOutput<TSchema>>();
  const updateExpression = buildUpdateExpression(updates, builder as any);

  const commandInput: any = {
    TableName: config.tableName,
    Key: dynamoKey,
    UpdateExpression: updateExpression,
    ReturnValues: options?.returnValues ?? 'ALL_NEW',
  };

  if (Object.keys(builder.names).length > 0) {
    commandInput.ExpressionAttributeNames = builder.names;
  }
  if (Object.keys(builder.values).length > 0) {
    commandInput.ExpressionAttributeValues = marshall(builder.values);
  }

  // Handle condition expression
  if (options?.condition) {
    applyConditionExpressionWithMerge<InferOutput<TSchema>>(options.condition, commandInput);
  }

  return new UpdateItemCommand(commandInput);
}

export function buildDeleteItemCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  // @ts-expect-error - KeyInput constraint issue with KeyConfig<TEntity>, works at runtime
  key: KeyInput<InferOutput<TSchema>, TKeyConfig>,
  options: DeleteOptions<InferOutput<TSchema>> | undefined,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): DeleteItemCommand {
  const dynamoKey = buildKey(key, config.key as any);

  const commandInput: any = {
    TableName: config.tableName,
    Key: dynamoKey,
  };

  // Handle condition expression
  if (options?.condition) {
    applyConditionExpression<InferOutput<TSchema>>(options.condition, commandInput);
  }

  return new DeleteItemCommand(commandInput);
}

export function buildQueryCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  // @ts-expect-error - PartitionKeyValue constraint issue with KeyConfig<TEntity>, works at runtime
  partitionKey: PartitionKeyValue<InferOutput<TSchema>, TKeyConfig>,
  options: QueryOptions<InferOutput<TSchema>, TKeyConfig['sk'] extends string ? true : false> | undefined,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): QueryCommand {
  const builder = createExpressionBuilder<InferOutput<TSchema>>();
  const keyConditionExpression = buildKeyConditionExpression(
    partitionKey as any,
    config.key,
    options,
    builder
  );

  const commandInput: any = {
    TableName: config.tableName,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeNames: builder.names,
    ExpressionAttributeValues: marshall(builder.values),
  };

  // Handle filter expression
  if (options?.filter) {
    applyFilterExpressionWithMerge<InferOutput<TSchema>>(options.filter, commandInput);
  }

  applyQueryOptions(options, commandInput);

  return new QueryCommand(commandInput);
}

export function buildQueryIndexCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  indexName: keyof NonNullable<TIndexes> & string,
  partitionKey: any,
  options: any,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): QueryCommand {
  if (!config.indexes || !(indexName in config.indexes)) {
    throw new Error(`Index "${indexName}" not found in table configuration`);
  }

  const indexConfig = config.indexes[indexName];
  const indexKey = indexConfig.key;
  const builder = createExpressionBuilder<InferOutput<TSchema>>();
  const keyConditionExpression = buildKeyConditionExpression(
    partitionKey as any,
    indexKey,
    options,
    builder
  );

  const commandInput: any = {
    TableName: config.tableName,
    IndexName: indexName,
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeNames: builder.names,
    ExpressionAttributeValues: marshall(builder.values),
  };

  // Handle filter expression
  if (options?.filter) {
    applyFilterExpressionWithMerge<InferOutput<TSchema>>(options.filter, commandInput);
  }

  applyQueryOptions(options, commandInput);

  return new QueryCommand(commandInput);
}

export function buildScanCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  options: ScanOptions<InferOutput<TSchema>> | undefined,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): ScanCommand {
  const commandInput: any = {
    TableName: config.tableName,
  };

  // Handle filter expression
  if (options?.filter) {
    applyFilterExpression<InferOutput<TSchema>>(options.filter, commandInput);
  }

  applyQueryOptions(options, commandInput);

  return new ScanCommand(commandInput);
}

export function buildTransactWriteItemsCommand<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  // @ts-expect-error - TransactionRequest constraint issue with KeyConfig<TEntity>, works at runtime
  requests: TransactionRequest<InferInput<TSchema>, TKeyConfig>[],
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): TransactWriteItemsCommand {
  if (requests.length > 25) {
    throw new Error('TransactWriteItems supports a maximum of 25 items per transaction');
  }

  const transactItems: any[] = [];

  for (const request of requests) {
    if (request.type === 'put') {
      const marshalledItem = marshall(request.item as Record<string, any>);
      const transactItem: any = {
        Put: {
          TableName: config.tableName,
          Item: marshalledItem,
        },
      };

      if (request.condition) {
        applyConditionExpression<InferInput<TSchema>>(request.condition, transactItem.Put);
      }

      transactItems.push(transactItem);
    } else if (request.type === 'update') {
      const dynamoKey = buildKey(request.key, config.key as any);
      const builder = createExpressionBuilder<InferOutput<TSchema>>();
      const updateExpression = buildUpdateExpression(request.updates, builder as any);

      const transactItem: any = {
        Update: {
          TableName: config.tableName,
          Key: dynamoKey,
          UpdateExpression: updateExpression,
        },
      };

      if (Object.keys(builder.names).length > 0) {
        transactItem.Update.ExpressionAttributeNames = builder.names;
      }
      if (Object.keys(builder.values).length > 0) {
        transactItem.Update.ExpressionAttributeValues = marshall(builder.values);
      }

      if (request.condition) {
        applyConditionExpressionWithMerge<InferOutput<TSchema>>(request.condition, transactItem.Update);
      }

      transactItems.push(transactItem);
    } else if (request.type === 'delete') {
      const dynamoKey = buildKey(request.key, config.key as any);
      const transactItem: any = {
        Delete: {
          TableName: config.tableName,
          Key: dynamoKey,
        },
      };

      if (request.condition) {
        applyConditionExpression<InferOutput<TSchema>>(request.condition, transactItem.Delete);
      }

      transactItems.push(transactItem);
    }
  }

  return new TransactWriteItemsCommand({
    TransactItems: transactItems,
  });
}

