import { unmarshall } from '@aws-sdk/util-dynamodb';
import type {
  GetItemCommandOutput,
  BatchGetItemCommandOutput,
  UpdateItemCommandOutput,
  QueryCommandOutput,
  ScanCommandOutput,
} from '@aws-sdk/client-dynamodb';
import type { StandardSchemaV1 } from '@standard-schema/spec';
import type {
  InferOutput,
  BatchGetResult,
  PaginateResult,
} from './types.js';
import { encodeExclusiveStartKey } from './utils.js';

export function toGetItemResult<TSchema extends StandardSchemaV1>(
  response: GetItemCommandOutput
): InferOutput<TSchema> | undefined {
  if (!response.Item) {
    return undefined;
  }
  return unmarshall(response.Item) as InferOutput<TSchema>;
}

export function toBatchGetResult<TSchema extends StandardSchemaV1>(
  response: BatchGetItemCommandOutput,
  tableName: string
): BatchGetResult<InferOutput<TSchema>> {
  const items: InferOutput<TSchema>[] = [];
  
  if (response.Responses && response.Responses[tableName]) {
    items.push(...response.Responses[tableName].map(item => 
      unmarshall(item) as InferOutput<TSchema>
    ));
  }

  return { items };
}

export function toPutItemResult<TSchema extends StandardSchemaV1>(
  item: any
): InferOutput<TSchema> {
  return item as InferOutput<TSchema>;
}

export function toUpdateItemResult<TSchema extends StandardSchemaV1>(
  response: UpdateItemCommandOutput
): InferOutput<TSchema> {
  if (!response.Attributes) {
    throw new Error('UpdateItem did not return attributes');
  }
  return unmarshall(response.Attributes) as InferOutput<TSchema>;
}

export function toQueryResult<TSchema extends StandardSchemaV1>(
  response: QueryCommandOutput
): InferOutput<TSchema>[] {
  if (!response.Items) {
    return [];
  }
  return response.Items.map(item => unmarshall(item) as InferOutput<TSchema>);
}

export function toScanResult<TSchema extends StandardSchemaV1>(
  response: ScanCommandOutput
): InferOutput<TSchema>[] {
  if (!response.Items) {
    return [];
  }
  return response.Items.map(item => unmarshall(item) as InferOutput<TSchema>);
}

export function toPaginateResult<TSchema extends StandardSchemaV1>(
  response: QueryCommandOutput | ScanCommandOutput
): PaginateResult<InferOutput<TSchema>> {
  const items = response.Items 
    ? response.Items.map((item: any) => unmarshall(item) as InferOutput<TSchema>)
    : [];

  const hasNextPage = !!response.LastEvaluatedKey;
  const nextPageToken = hasNextPage ? encodeExclusiveStartKey(response.LastEvaluatedKey) : undefined;

  return {
    data: items,
    hasNextPage,
    nextPageToken,
  };
}

export function toTransactionResult<TSchema extends StandardSchemaV1>(
  requests: any[]
): { items: InferOutput<TSchema>[] } {
  const items: InferOutput<TSchema>[] = [];

  // For put and update operations, we return the items
  // Note: TransactWriteItems doesn't return items by default, so we return the input items
  for (const request of requests) {
    if (request.type === 'put') {
      items.push(request.item as InferOutput<TSchema>);
    } else if (request.type === 'update') {
      // For updates, we'd need to do a separate get or use ReturnValuesOnConditionCheckFailure
      // For now, we'll return a partial item with the key and updates merged
      items.push({ ...request.key, ...request.updates } as InferOutput<TSchema>);
    }
  }

  return { items };
}

