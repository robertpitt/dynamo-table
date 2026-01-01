import { marshall } from '@aws-sdk/util-dynamodb';
import type { KeyConfig, KeyConfigBase } from './types.js';
import type { ExpressionBuilderWithState } from './expression.js';
/**
 * Builds a DynamoDB key from a typed key input
 */
export function buildKey<TEntity, TKeyConfig extends KeyConfig<TEntity>>(
  key: any,
  keyConfig: any
): Record<string, any> {
  const dynamoKey: Record<string, any> = {};
  // Type assertion: KeyConfig pk/sk are always string keys in practice
  const keyConfigBase: KeyConfigBase = keyConfig;
  const pk = keyConfigBase.pk;
  const sk = keyConfigBase.sk;
  
  dynamoKey[pk] = key[keyConfig.pk];
  if (sk) {
    dynamoKey[sk] = key[keyConfig.sk];
  }
  return marshall(dynamoKey);
}

/**
 * Builds an UpdateExpression from an updates object
 */
export function buildUpdateExpression<TEntity, TKeyConfig extends KeyConfig<TEntity>>(
  updates: any,
  builder: ExpressionBuilderWithState<TEntity>
): string {
  const setExpressions: string[] = [];
  const removeExpressions: string[] = [];
  let valueCounter = 0;

  for (const [key, value] of Object.entries(updates)) {
    // Handle nested attribute paths (e.g., "address.city")
    const segments = key.split('.');
    const attrNameParts = segments.map(seg => {
      const nameKey = `#${seg}`;
      if (!builder.names[nameKey]) {
        builder.names[nameKey] = seg;
      }
      return nameKey;
    });
    const attrName = attrNameParts.join('.');

    if (value === undefined || value === null) {
      // REMOVE expression
      removeExpressions.push(attrName);
    } else {
      // SET expression
      const valName = `:val${valueCounter++}`;
      builder.values[valName] = value;
      setExpressions.push(`${attrName} = ${valName}`);
    }
  }

  const parts: string[] = [];
  if (setExpressions.length > 0) {
    parts.push(`SET ${setExpressions.join(', ')}`);
  }
  if (removeExpressions.length > 0) {
    parts.push(`REMOVE ${removeExpressions.join(', ')}`);
  }

  if (parts.length === 0) {
    throw new Error('UpdateExpression cannot be empty');
  }

  return parts.join(' ');
}

/**
 * Validates and parses exclusive start key from string or object
 */
export function parseExclusiveStartKey(exclusiveStartKey?: string | Record<string, any>): Record<string, any> | undefined {
  if (!exclusiveStartKey) return undefined;
  if (typeof exclusiveStartKey === 'string') {
    try {
      return JSON.parse(Buffer.from(exclusiveStartKey, 'base64').toString('utf-8'));
    } catch {
      throw new Error('Invalid exclusiveStartKey: must be a valid base64-encoded JSON string');
    }
  }
  return exclusiveStartKey;
}

/**
 * Encodes exclusive start key to string token
 */
export function encodeExclusiveStartKey(key?: Record<string, any>): string | undefined {
  if (!key) return undefined;
  return Buffer.from(JSON.stringify(key)).toString('base64');
}


