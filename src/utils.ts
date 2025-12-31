import type { QueryCommandOutput, ScanCommandOutput } from '@aws-sdk/lib-dynamodb';
import type { KeyConfig, KeyInput, PagedResult } from './types.js';

// ============================================================================
// Key Utilities
// ============================================================================

/**
 * Extracts the DynamoDB key object from an entity or key input based on key configuration.
 */
export const extractKey = <Entity, Key extends KeyConfig>(
  entityOrKey: Entity | KeyInput<Entity, Key>,
  keyConfig: Key
): Record<string, unknown> => {
  const record = entityOrKey as Record<string, unknown>;
  const key: Record<string, unknown> = {
    [keyConfig.pk]: record[keyConfig.pk],
  };

  if (keyConfig.sk !== undefined) {
    key[keyConfig.sk] = record[keyConfig.sk];
  }

  return key;
};

/**
 * Validates that a key input contains all required key fields.
 */
export const validateKeyInput = <Entity, Key extends KeyConfig>(
  keyInput: KeyInput<Entity, Key>,
  keyConfig: Key
): void => {
  const input = keyInput as Record<string, unknown>;

  if (input[keyConfig.pk] == null) {
    throw new Error(`Missing required partition key field: ${keyConfig.pk}`);
  }

  if (keyConfig.sk !== undefined && input[keyConfig.sk] == null) {
    throw new Error(`Missing required sort key field: ${keyConfig.sk}`);
  }
};

/**
 * Extracts and validates a key in one operation.
 */
export const extractAndValidateKey = <Entity, Key extends KeyConfig>(
  keyInput: KeyInput<Entity, Key>,
  keyConfig: Key
): Record<string, unknown> => {
  validateKeyInput(keyInput, keyConfig);
  return extractKey(keyInput, keyConfig);
};

// ============================================================================
// Expression Builders
// ============================================================================

/**
 * Builds a key condition expression for DynamoDB queries.
 */
export const buildKeyConditionExpression = (
  pkField: string,
  pkValue: string,
  skField?: string,
  skCondition?: string
): {
  KeyConditionExpression: string;
  ExpressionAttributeNames: Record<string, string>;
  ExpressionAttributeValues: Record<string, unknown>;
} => {
  const expressionAttributeNames: Record<string, string> = {
    '#pk': pkField,
  };
  const expressionAttributeValues: Record<string, unknown> = {
    ':pk': pkValue,
  };

  let keyConditionExpression = '#pk = :pk';

  if (skCondition && skField) {
    expressionAttributeNames['#sk'] = skField;
    keyConditionExpression += ` AND ${skCondition}`;
  }

  return {
    KeyConditionExpression: keyConditionExpression,
    ExpressionAttributeNames: expressionAttributeNames,
    ExpressionAttributeValues: expressionAttributeValues,
  };
};

/**
 * Builds an update expression from partial entity updates.
 */
export const buildUpdateExpression = <Entity>(
  updates: Partial<Entity> | Record<string, unknown>
): {
  UpdateExpression: string;
  ExpressionAttributeNames: Record<string, string>;
  ExpressionAttributeValues: Record<string, unknown>;
} => {
  const updatesRecord = updates as Record<string, unknown>;
  const allKeys = Object.keys(updatesRecord);

  // Separate keys into those to set (non-null values) and those to remove (null values)
  const keysToSet = allKeys.filter(
    (key) => updatesRecord[key] !== undefined && updatesRecord[key] !== null
  );
  const keysToRemove = allKeys.filter((key) => updatesRecord[key] === null);

  // Build SET clause for non-null values
  const setClauses = keysToSet.map((key) => `#${key} = :${key}`);
  const setExpression = setClauses.length > 0 ? `SET ${setClauses.join(', ')}` : '';

  // Build REMOVE clause for null values
  const removeClauses = keysToRemove.map((key) => `#${key}`);
  const removeExpression = removeClauses.length > 0 ? `REMOVE ${removeClauses.join(', ')}` : '';

  // Combine expressions
  const updateExpression = [setExpression, removeExpression].filter(Boolean).join(' ').trim();

  // Build expression attribute names for all modified keys
  const allModifiedKeys = [...keysToSet, ...keysToRemove];
  const expressionAttributeNames = Object.fromEntries(
    allModifiedKeys.map((key) => [`#${key}`, key])
  );

  // Build expression attribute values only for SET operations
  const expressionAttributeValues = Object.fromEntries(
    keysToSet.map((key) => [`:${key}`, updatesRecord[key]])
  );

  return {
    UpdateExpression: updateExpression || '',
    ExpressionAttributeNames: expressionAttributeNames,
    ExpressionAttributeValues: expressionAttributeValues,
  };
};

/**
 * Normalizes expression attributes by removing empty objects.
 */
export const normalizeExpressionAttributes = <T extends Record<string, unknown>>(
  attrs: T
): T | undefined => (Object.keys(attrs).length > 0 ? attrs : undefined);

/**
 * Merges and normalizes multiple expression attribute objects.
 */
export const mergeAndNormalize = <T extends Record<string, unknown> | undefined>(
  ...parts: Array<T>
): Record<string, unknown> | undefined => {
  const merged = Object.assign({}, ...parts.filter(Boolean));
  const normalized = normalizeExpressionAttributes(merged);
  return Object.keys(normalized).length ? normalized : undefined;
};

/**
 * Merges expression attributes, with later values taking precedence.
 * Useful for combining expression attribute names or values from multiple sources.
 *
 * @example
 * ```typescript
 * const names1 = { '#name': 'name' };
 * const names2 = { '#email': 'email' };
 * const merged = mergeExpressionAttributes(names1, names2);
 * // Result: { '#name': 'name', '#email': 'email' }
 * ```
 *
 * @param attrs - Variable number of attribute objects to merge
 * @returns Merged attributes object with later values taking precedence
 */
export const mergeExpressionAttributes = (
  ...attrs: Array<Record<string, unknown> | undefined>
): Record<string, unknown> => {
  const merged: Record<string, unknown> = {};

  for (const attr of attrs) {
    if (attr) {
      Object.assign(merged, attr);
    }
  }

  return merged;
};

// ============================================================================
// Pagination Utilities
// ============================================================================

/**
 * Encodes a pagination key to base64 string.
 */
export const encodePageKey = (key: Record<string, unknown>): string => {
  return Buffer.from(JSON.stringify(key)).toString('base64');
};

/**
 * Decodes a base64 pagination key string.
 */
export const decodePageKey = (encodedKey: string): Record<string, unknown> => {
  return JSON.parse(Buffer.from(encodedKey, 'base64').toString()) as Record<string, unknown>;
};

// ============================================================================
// Response Transformers
// ============================================================================

/**
 * Converts a DynamoDB query/scan response to a PagedResult.
 */
export const toPagedResult = <Entity>(
  result: QueryCommandOutput | ScanCommandOutput
): PagedResult<Entity> => {
  const hasNextPage = result.LastEvaluatedKey !== undefined;
  const nextPageKey =
    hasNextPage && result.LastEvaluatedKey ? encodePageKey(result.LastEvaluatedKey) : undefined;

  return {
    hasNextPage,
    nextPageKey,
    items: (result.Items as Entity[]) ?? [],
  };
};

// ============================================================================
// Batch Operations
// ============================================================================

/**
 * Maps batch write requests to DynamoDB write requests.
 */
export const mapBatchWriteRequests = <Entity>(
  requests: Array<{ type: 'put'; item: Entity } | { type: 'delete'; key: Record<string, unknown> }>
): Array<
  | { PutRequest: { Item: Record<string, unknown> } }
  | { DeleteRequest: { Key: Record<string, unknown> } }
> =>
  requests.map((request) =>
    request.type === 'put'
      ? { PutRequest: { Item: request.item as Record<string, unknown> } }
      : { DeleteRequest: { Key: request.key } }
  );

/**
 * Maps transaction write requests to DynamoDB transaction items.
 */
export const mapTransactItems = <Entity>(
  tableName: string,
  requests: Array<
    | { type: 'put'; item: Entity; conditionExpression?: string }
    | {
        type: 'update';
        key: Record<string, unknown>;
        updateExpression: string;
        expressionAttributeNames?: Record<string, string>;
        expressionAttributeValues?: Record<string, unknown>;
        conditionExpression?: string;
      }
    | { type: 'delete'; key: Record<string, unknown>; conditionExpression?: string }
  >
): Array<
  | { Put: { TableName: string; Item: Record<string, unknown>; ConditionExpression?: string } }
  | {
      Update: {
        TableName: string;
        Key: Record<string, unknown>;
        UpdateExpression: string;
        ExpressionAttributeNames?: Record<string, string>;
        ExpressionAttributeValues?: Record<string, unknown>;
        ConditionExpression?: string;
      };
    }
  | { Delete: { TableName: string; Key: Record<string, unknown>; ConditionExpression?: string } }
> =>
  requests.map((request) => {
    if (request.type === 'put') {
      return {
        Put: {
          TableName: tableName,
          Item: request.item as Record<string, unknown>,
          ConditionExpression: request.conditionExpression,
        },
      };
    }
    if (request.type === 'update') {
      return {
        Update: {
          TableName: tableName,
          Key: request.key,
          UpdateExpression: request.updateExpression,
          ExpressionAttributeNames: request.expressionAttributeNames,
          ExpressionAttributeValues: request.expressionAttributeValues,
          ConditionExpression: request.conditionExpression,
        },
      };
    }
    return {
      Delete: {
        TableName: tableName,
        Key: request.key,
        ConditionExpression: request.conditionExpression,
      },
    };
  });

/**
 * Creates a deterministic string key from a DynamoDB key object for comparison.
 */
const createKeyString = (key: Record<string, unknown>, keyConfig: KeyConfig): string => {
  const pk = String(key[keyConfig.pk] ?? '');
  const sk = keyConfig.sk !== undefined ? String(key[keyConfig.sk] ?? '') : '';
  return keyConfig.sk !== undefined ? `${pk}::${sk}` : pk;
};

/**
 * Sorts batch get results to match input key order.
 */
export const sortBatchResults = <Entity, Key extends KeyConfig>(
  keys: KeyInput<Entity, Key>[],
  items: Entity[],
  keyConfig: Key
): Entity[] => {
  const keyMap = new Map(
    items.map((item) => {
      const key = extractKey(item, keyConfig);
      return [createKeyString(key, keyConfig), item];
    })
  );

  return keys
    .map((key) => {
      const dynamoKey = extractKey(key, keyConfig);
      return keyMap.get(createKeyString(dynamoKey, keyConfig));
    })
    .filter((item): item is Entity => item !== undefined);
};

// ============================================================================
// Sort Key Condition Helpers
// ============================================================================

/**
 * Helper functions for building sort key conditions in DynamoDB queries.
 * These return condition expression strings that can be used with `buildKeyConditionExpression`
 * or passed directly as the `skCondition` parameter in query options.
 *
 * Note: When using these helpers, you must provide the corresponding expression attribute values
 * in your query options. For example:
 *
 * @example
 * ```typescript
 * // Using beginsWith condition
 * await table.User.query({
 *   pk: 'USER#123',
 *   skCondition: sortKeyConditions.beginsWith(),
 *   ExpressionAttributeValues: {
 *     ':sk_prefix': 'ORDER#'
 *   }
 * });
 *
 * // Using between condition
 * await table.User.query({
 *   pk: 'USER#123',
 *   skCondition: sortKeyConditions.between(),
 *   ExpressionAttributeValues: {
 *     ':sk_start': '2024-01-01',
 *     ':sk_end': '2024-12-31'
 *   }
 * });
 * ```
 */
export const sortKeyConditions = {
  /** Sort key equals condition: `#sk = :sk` */
  equals: (): string => '#sk = :sk',
  /** Sort key begins with condition: `begins_with(#sk, :sk_prefix)` */
  beginsWith: (): string => 'begins_with(#sk, :sk_prefix)',
  /** Sort key between condition: `#sk BETWEEN :sk_start AND :sk_end` */
  between: (): string => '#sk BETWEEN :sk_start AND :sk_end',
  /** Sort key less than condition: `#sk < :sk` */
  lessThan: (): string => '#sk < :sk',
  /** Sort key less than or equal condition: `#sk <= :sk` */
  lessThanOrEqual: (): string => '#sk <= :sk',
  /** Sort key greater than condition: `#sk > :sk` */
  greaterThan: (): string => '#sk > :sk',
  /** Sort key greater than or equal condition: `#sk >= :sk` */
  greaterThanOrEqual: (): string => '#sk >= :sk',
} as const;
