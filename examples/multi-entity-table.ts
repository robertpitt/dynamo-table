/**
 * Example: Single-Entity Table with Valibot Schema
 *
 * This example demonstrates using itty-repo with a single-entity table design
 * matching the ideal DX from idea.ts.
 */

import * as v from 'valibot';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { table } from '../src/index.js';

// Define entity schema using Valibot
const AuditSchema = v.object({
    id: v.pipe(v.string(), v.uuid()),
    userId: v.pipe(v.string(), v.uuid()),
    accountId: v.pipe(v.string(), v.uuid()),
    resellerId: v.pipe(v.string(), v.uuid()),
    requestBody: v.string(),
    requestHeaders: v.object({}),
    responseBody: v.string(),
    responseHeaders: v.object({}),
    responseCode: v.number(),
    updatedAt: v.string(),
    createdAt: v.string(),
    ttl: v.optional(v.number()),
});

const dynamo = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' })

// Create table with single entity schema
const audits = table(dynamo, {
    tableName: process.env.AUDITS_TABLE_NAME!,
    schema: AuditSchema,
    key: { pk: 'id' },
    ttl: 'ttl',
    indexes: {
        auditsByUserId: { key: { pk: 'userId', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
        auditsByAccountId: { key: { pk: 'accountId', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
        auditsByResellerId: { key: { pk: 'resellerId', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
        auditsByStatus: {
            key: { pk: 'responseCode', sk: 'createdAt' },
            projection: { include: ['id', 'responseCode', 'createdAt', 'resellerId', 'accountId', 'userId'] },
        },
    },
});


audits.get({ id: "123" })
audits.query({ id: "test" })
audits.batchGet([{ id: "test" }])
audits.put({ id: "test", userId: "test", accountId: "test", resellerId: "test", requestBody: "test", requestHeaders: {}, responseBody: "test", responseHeaders: {}, responseCode: 200, updatedAt: "test", createdAt: "test", ttl: 1000 })
audits.update({ id: "test" }, { responseCode: 200, updatedAt: "test" })
audits.delete({ id: "test" })
audits.queryIndex('auditsByUserId', { userId: "test" })
audits.queryIndex('auditsByAccountId', { accountId: "test" })
audits.queryIndex('auditsByResellerId', { resellerId: "test" }, {
  sortKey: (sk) => sk.between("2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z")
})
audits.queryIndex('auditsByStatus', { responseCode: 200 }, {
  sortKey: (sk) => sk.between("2025-01", "2025-01")
})
audits.queryIndex("auditsByResellerId", { resellerId: "test" })
audits.scan({ filter: (f) => f.gt('responseCode', 399), limit: 100 })
audits.paginate({ filter: (f) => f.gt('responseCode', 399), limit: 100 })

const UserSchema = v.object({
  id: v.pipe(v.string(), v.uuid()),
  orgId: v.pipe(v.string(), v.uuid()),
  name: v.string(),
  email: v.pipe(v.string(), v.email()),
  phone: v.pipe(v.string()),
  address: v.object({
    street: v.string(),
    city: v.string(),
    state: v.string(),
    zip: v.string(),
  }),
  dob: v.pipe(v.string(), v.isoDateTime()),
  gender: v.picklist(['male', 'female', 'other']),
  status: v.picklist(['active', 'inactive', 'pending']),
  role: v.picklist(['admin', 'user', 'superadmin']),
  permissions: v.array(v.string()),
  tags: v.array(v.string()),
    metadata: v.array(v.object({
        key: v.string(),
        value: v.string(),
    })),
  createdAt: v.optional(v.pipe(v.string(), v.isoDateTime()), () => new Date().toISOString()),
  updatedAt: v.optional(v.pipe(v.string(), v.isoDateTime()), () => new Date().toISOString()),
  ttl: v.optional(v.number()),
});

const users = table(dynamo, {
  tableName: process.env.USERS_TABLE_NAME!,
  schema: UserSchema,
  key: { pk: 'orgId', sk: 'id' },
  ttl: 'ttl',
  indexes: {
    usersByEmail: { key: { pk: 'email', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
    usersByStatus: { key: { pk: 'status', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
    usersByRole: { key: { pk: 'role', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
    usersByGender: { key: { pk: 'gender', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
    usersByDob: { key: { pk: 'dob', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
    usersByAddress: { key: { pk: 'address', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
    usersByPhone: { key: { pk: 'phone', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
    usersByName: { key: { pk: 'name', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
    usersByPermissions: { key: { pk: 'permissions', sk: 'createdAt' }, projection: 'KEYS_ONLY' },
  },
  methods: {},
});

users.get({ id: "456", orgId: "123" }).then(result => result?.address.city);

users.batchGet([{orgId: "123", id: "456"}, {orgId: "123", id: "789"}]).then(
    result => result.items.map(item => item.address.city)
);

users.delete({ id: "456", orgId: "123" })

users.queryIndex("usersByName", { name: "Test" }, {
  sortKey: (sk) => sk.beginsWith("John"),
  filter: (f) => f.and(
    f.eq("permissions[0]", ""),
    f.eq("tags[0]", ""),
    f.contains("metadata.key", "test"),
    f.eq("metadata.value", ""),
    f.eq('address.city', "New York")
  )
})