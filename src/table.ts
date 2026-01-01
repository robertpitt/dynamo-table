import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import type { StandardSchemaV1 } from '@standard-schema/spec';
import type {
  TableOptions,
  Table,
  KeyConfig,
  InferOutput,
  InferInput,
  ExtendedTableOptions,
  BatchGetResult,
  PaginateResult,
  KeyInput,
  PartitionKeyValue,
  UpdateInput,
  GetOptions,
  PutOptions,
  UpdateOptions,
  DeleteOptions,
  QueryOptions,
  ScanOptions,
  PaginateOptions,
  TransactionRequest,
} from './types.js';
import {
  buildGetItemCommand,
  buildBatchGetItemCommand,
  buildPutItemCommand,
  buildUpdateItemCommand,
  buildDeleteItemCommand,
  buildQueryCommand,
  buildQueryIndexCommand,
  buildScanCommand,
  buildTransactWriteItemsCommand,
} from './commands.js';
import {
  toGetItemResult,
  toBatchGetResult,
  toPutItemResult,
  toUpdateItemResult,
  toQueryResult,
  toScanResult,
  toPaginateResult,
  toTransactionResult,
} from './results.js';
import { parseExclusiveStartKey } from './utils.js';

function get<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async (
    // @ts-expect-error - KeyInput constraint issue with KeyConfig<TEntity>, works at runtime
    key: KeyInput<InferOutput<TSchema>, TKeyConfig>,
    options?: GetOptions
  ): Promise<InferOutput<TSchema> | undefined> => {
    const command = buildGetItemCommand(key, options, config);
    const response = await client.send(command);
    return toGetItemResult<TSchema>(response);
  };
}

function batchGet<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async (
    // @ts-expect-error - KeyInput constraint issue with KeyConfig<TEntity>, works at runtime
    keys: KeyInput<InferOutput<TSchema>, TKeyConfig>[],
    options?: GetOptions
  ): Promise<BatchGetResult<InferOutput<TSchema>>> => {
    if (keys.length === 0) {
      return { items: [] };
    }

    // BatchGetItem has a limit of 100 items per request
    const batchSize = 100;
    const allItems: InferOutput<TSchema>[] = [];

    for (let i = 0; i < keys.length; i += batchSize) {
      const batch = keys.slice(i, i + batchSize);
      const command = buildBatchGetItemCommand(batch, options, config);
      const response = await client.send(command);

      const result = toBatchGetResult<TSchema>(response, config.tableName);
      allItems.push(...result.items);

      // Handle unprocessed keys (retry logic could be added here)
      if (response.UnprocessedKeys && Object.keys(response.UnprocessedKeys).length > 0) {
        // For now, we'll just log a warning. In production, you might want to retry.
        console.warn('Some keys were unprocessed in batchGet');
      }
    }

    return { items: allItems };
  };
}

function put<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async (
    item: InferInput<TSchema>,
    options?: PutOptions<InferInput<TSchema>>
  ): Promise<InferOutput<TSchema>> => {
    const command = buildPutItemCommand(item, options, config);
    await client.send(command);
    return toPutItemResult<TSchema>(item);
  };
}

function update<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async (
    // @ts-expect-error - KeyInput constraint issue with KeyConfig<TEntity>, works at runtime
    key: KeyInput<InferOutput<TSchema>, TKeyConfig>,
    // @ts-expect-error - UpdateInput constraint issue with KeyConfig<TEntity>, works at runtime
    updates: UpdateInput<InferOutput<TSchema>, TKeyConfig>,
    options?: UpdateOptions<InferOutput<TSchema>>
  ): Promise<InferOutput<TSchema>> => {
    const command = buildUpdateItemCommand(key, updates, options, config);
    const response = await client.send(command);
    return toUpdateItemResult<TSchema>(response);
  };
}

function remove<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async (
    // @ts-expect-error - KeyInput constraint issue with KeyConfig<TEntity>, works at runtime
    key: KeyInput<InferOutput<TSchema>, TKeyConfig>,
    options?: DeleteOptions<InferOutput<TSchema>>
  ): Promise<void> => {
    const command = buildDeleteItemCommand(key, options, config);
    await client.send(command);
  };
}

function query<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async (
    // @ts-expect-error - PartitionKeyValue constraint issue with KeyConfig<TEntity>, works at runtime
    partitionKey: PartitionKeyValue<InferOutput<TSchema>, TKeyConfig>,
    options?: QueryOptions<InferOutput<TSchema>, TKeyConfig['sk'] extends string ? true : false>
  ): Promise<InferOutput<TSchema>[]> => {
    const command = buildQueryCommand(partitionKey, options, config);
    const response = await client.send(command);
    return toQueryResult<TSchema>(response);
  };
}

function queryIndex<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async <TIndexName extends keyof NonNullable<TIndexes> & string>(
    indexName: TIndexName,
    partitionKey: PartitionKeyValue<
      InferOutput<TSchema>,
      // @ts-expect-error - KeyConfig constraint issue, works at runtime
      NonNullable<TIndexes>[TIndexName]['key']
    >,
    options?: NonNullable<TIndexes>[TIndexName]['key']['sk'] extends string
      ? QueryOptions<InferOutput<TSchema>, true>
      : QueryOptions<InferOutput<TSchema>, false>
  ): Promise<InferOutput<TSchema>[]> => {
    const command = buildQueryIndexCommand(indexName, partitionKey, options, config);
    const response = await client.send(command);
    return toQueryResult<TSchema>(response);
  };
}

function scan<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async (
    options?: ScanOptions<InferOutput<TSchema>>
  ): Promise<InferOutput<TSchema>[]> => {
    const command = buildScanCommand(options, config);
    const response = await client.send(command);
    return toScanResult<TSchema>(response);
  };
}

function paginate<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async function* (
    options: QueryOptions<InferOutput<TSchema>> | ScanOptions<InferOutput<TSchema>>,
    paginateOptions?: PaginateOptions<InferOutput<TSchema>>
  ): AsyncGenerator<PaginateResult<InferOutput<TSchema>>, void, unknown> {
    let exclusiveStartKey: Record<string, any> | undefined = parseExclusiveStartKey(options.exclusiveStartKey);
    const pageLimit = paginateOptions?.limit ?? options.limit;

    while (true) {
      // For now, paginate only supports scan operations
      const scanOptions: ScanOptions<InferOutput<TSchema>> = {
        ...(options as ScanOptions<InferOutput<TSchema>>),
        limit: pageLimit,
        exclusiveStartKey: exclusiveStartKey,
      };

      const command = buildScanCommand(scanOptions, config);
      const response = await client.send(command);

      const result = toPaginateResult<TSchema>(response);

      yield result;

      if (!result.hasNextPage) {
        break;
      }

      exclusiveStartKey = response.LastEvaluatedKey;
    }
  };
}

function transaction<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>>,
  TIndexes extends TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
) {
  return async (
    // @ts-expect-error - TransactionRequest constraint issue with KeyConfig<TEntity>, works at runtime
    requests: TransactionRequest<InferInput<TSchema>, TKeyConfig>[]
  ): Promise<{ items: InferOutput<TSchema>[] }> => {
    if (requests.length === 0) {
      return { items: [] };
    }

    const command = buildTransactWriteItemsCommand(requests, config);
    await client.send(command);
    return toTransactionResult<TSchema>(requests);
  };
}

export function table<
  TSchema extends StandardSchemaV1,
  TKeyConfig extends KeyConfig<InferOutput<TSchema>> = TableOptions<TSchema>['key'],
  TIndexes extends TableOptions<TSchema>['indexes'] = TableOptions<TSchema>['indexes']
>(
  client: DynamoDBClient,
  config: ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>
): Table<ExtendedTableOptions<TSchema, TKeyConfig, TIndexes>> {
  return {
    get: get(client, config),
    put: put(client, config),
    update: update(client, config),
    delete: remove(client, config),
    query: query(client, config),
    queryIndex: queryIndex(client, config),
    scan: scan(client, config),
    batchGet: batchGet(client, config),
    paginate: paginate(client, config),
    transaction: transaction(client, config),
  };
};
