[![Apache License](https://img.shields.io/github/license/emdgroup/dynamodb-paginator.svg?style=flat-square)](https://github.com/emdgroup/dynamodb-paginator/blob/master/LICENSE)
[![Sponsored by EMD Group](https://img.shields.io/badge/sponsored%20by-emdgroup.com-ff55aa.svg?style=flat-square)](http://emdgroup.com)
[![Downloads](https://img.shields.io/npm/dw/@emdgroup/dynamodb-paginator.svg?style=flat-square)](https://www.npmjs.com/package/@emdgroup/dynamodb-paginator)

# DynamoDB Paginator

Features:
  * Supports Binary and String key types
  * Generates AES256 encrypted and authenticated pagination tokens
  * Works with TypeScript type guards natively
  * Ensures a minimum number of items when using a `FilterExpression`
  * Compatible with AWS SDK v2 and v3

Pagination in NoSQL stores such as DynamoDB can be challenging. This
library provides a developer friendly interface around the DynamoDB `Query` and `Scan` APIs.
It generates and encrypted and authenticated pagination token that can be shared with an untrustworthy
client (like the browser or a mobile app) without disclosing potentially sensitive data and protecting
the integrity of the token.

**Why is the pagination token encrypted?**

When researching pagination with DynamoDB, you will come across blog posts and libraries that recommend
to JSON-encode the `LastEvaluatedKey` attribute (or even the whole query command). **This is dangerous!**

The token is sent to a client which can happily decode the token, look at the values for the
partition and sort key and even modify the token, making the application vulnerable to NoSQL injections.

> "Dance like nobody is watching. Encrypt like everyone is."
> -- Werner Vogels

## Usage

```typescript
import { Paginator } from '@emdgroup/dynamodb-paginator';
import { DynamoDB } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

import * as crypto from 'crypto';

const client = DynamoDBDocument.from(new DynamoDB({}));
// persist the key in the SSM parameter store or similar
const key = crypto.randomBytes(32);

const paginateQuery = Paginator.createQuery({
  key,
  client,
});

const paginator = paginateQuery({
  TableName: 'mytable',
  KeyConditionExpression: 'PK = :pk',
  ExpressionAttributeValues: {
      ':pk': 'U#ABC',
  },
});
```

Use `for await...of` syntax:

```typescript
for await (const item of paginator) {
  // do something with item

  // only work on the first 50 items,
  // then generate a pagination token and break.
  if (paginator.count === 50) {
    console.log(paginator.nextToken);
    break;
  }
}

items.requestCount; // number of requests to DynamoDB
```

Use `await all()` syntax:

```typescript
const items = await paginator.limit(50).all();
paginator.nextToken;

const nextPaginator = paginator.from(paginator.nextToken);
nextPaginator.all(); // up to 50 more items
```

Use TypeScript guards to filter for items:

```typescript
interface User {
  PK: string;
  SK: string;
}

function isUser(arg: Record<string, unknown>): args is User {
  return typeof arg.PK === 'string' && 
    typeof arg.SK === 'string' &&
    arg.PK.startsWith('U#');
}

for await (const user of paginator.filter(isUser)) {
  // user is of type User
}
```

## Paginator

The `Paginator` class is a factory for the [`PaginationResponse`](#PaginationResponse) object. This class
is instantiated with the 32-byte encryption key and the DynamoDB document client (versions
2 and 3 of the AWS SDK are supported).

```typescript
const paginateQuery = Paginator.createQuery({
  key: () => Promise.resolve(crypto.randomBytes(32)),
  client: documentClient,
});
```

To create a paginator over a scan operation, use `createScan`.

```typescript
const paginateScan = Paginator.createScan({
  key: () => Promise.resolve(crypto.randomBytes(32)),
  client: documentClient,
});
```

## Constructors

### constructor

• **new Paginator**(`args`)

Use the static factory function [`create()`](#create) instead of the constructor.

#### Parameters

| Name | Type |
| :------ | :------ |
| `args` | [`PaginatorOptions`](#PaginatorOptions) |

## Methods

### createQuery

▸ `Static` **createQuery**(`args`): <T\>(`query`: `QueryCommandInput`, `opts?`: [`PaginateQueryOptions`](#PaginateQueryOptions)<`T`\>) => [`PaginationResponse`](#PaginationResponse)<`T`\>

Returns a function that accepts a DynamoDB Query command and return an instance of `PaginationResponse`.

#### Parameters

| Name | Type |
| :------ | :------ |
| `args` | [`PaginatorOptions`](#PaginatorOptions) |

#### Returns

`fn`

▸ <`T`\>(`query`, `opts?`): [`PaginationResponse`](#PaginationResponse)<`T`\>

Returns a function that accepts a DynamoDB Query command and return an instance of `PaginationResponse`.

##### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | extends `AttributeMap` |

##### Parameters

| Name | Type |
| :------ | :------ |
| `query` | `QueryCommandInput` |
| `opts?` | [`PaginateQueryOptions`](#PaginateQueryOptions)<`T`\> |

##### Returns

[`PaginationResponse`](#PaginationResponse)<`T`\>

___

### createScan

▸ `Static` **createScan**(`args`): <T\>(`scan`: `ScanCommandInput`, `opts?`: [`PaginateQueryOptions`](#PaginateQueryOptions)<`T`\>) => [`PaginationResponse`](#PaginationResponse)<`T`\>

Returns a function that accepts a DynamoDB Scan command and return an instance of `PaginationResponse`.

#### Parameters

| Name | Type |
| :------ | :------ |
| `args` | [`PaginatorOptions`](#PaginatorOptions) |

#### Returns

`fn`

▸ <`T`\>(`scan`, `opts?`): [`PaginationResponse`](#PaginationResponse)<`T`\>

Returns a function that accepts a DynamoDB Scan command and return an instance of `PaginationResponse`.

##### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | extends `AttributeMap` |

##### Parameters

| Name | Type |
| :------ | :------ |
| `scan` | `ScanCommandInput` |
| `opts?` | [`PaginateQueryOptions`](#PaginateQueryOptions)<`T`\> |

##### Returns

[`PaginationResponse`](#PaginationResponse)<`T`\>


## PaginatorOptions

## Properties

### client

• **client**: `DynamoDBDocumentClientV2` \| `DynamoDBDocumentClient`

AWS SDK v2 or v3 DynamoDB Document Client.

___

### indexes

• `Optional` **indexes**: `Record`<`string`, [partitionKey: string, sortKey?: string]\> \| (`index`: `string`) => [partitionKey: string, sortKey?: string]

Object that resolves an index name to the partition and sort key for that index.
Also accepts a function that builds the names based on the index name.

Defaults to ```(index) => [`${index}PK`, `${index}PK`]```.

___

### key

• **key**: `CipherKey` \| `Promise`<`CipherKey`\> \| () => `CipherKey` \| `Promise`<`CipherKey`\>

A 32-byte encryption key (e.g. `crypto.randomBytes(32)`). The `key` parameter also
accepts a Promise that resolves to a key or a function that resolves to a Promise of a key.

If a function is passed, that function is only called once. The function is called concurrently
with the first query request to DynamoDB to reduce the overall latency for the first query. The
key is cached and the function is not called again.

___

### schema

• `Optional` **schema**: [partitionKey: string, sortKey?: string]

Names for the partition and sort key of the table. Defaults to `['PK', 'SK']`.


## PaginationResponse

The `PaginationResponse` class implements the query result iterator. It has a number of
utility functions such as [`peek()`](#peek) and [`all()`](#all) to simplify common usage patterns.

The iterator can be interrupted and resumed at any time. The iterator will stop to produce
items after the end of the query is reached or the provided [`limit`](#limit) parameter is exceeded.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | extends `AttributeMap` = `AttributeMap` |

## Properties

### consumedCapacity

• **consumedCapacity**: `number`

Total consumed capacity for query

___

### count

• **count**: `number`

Number of items yielded

___

### requestCount

• **requestCount**: `number`

Number of requests made to DynamoDB

___

### scannedCount

• **scannedCount**: `number`

Number of items scanned by DynamoDB

## Accessors

### finished

• `get` **finished**(): `boolean`

Returns true if all items for this query have been returned from DynamoDB.

#### Returns

`boolean`

___

### nextToken

• `get` **nextToken**(): `undefined` \| `string`

Token to resume query operation from the current position. The token is generated from the `LastEvaluatedKey`
attribute provided by DynamoDB and then AES256 encrypted such that it can safely be provided to an
untrustworthy client (such as a user browser or mobile app). The token is Base64 URL encoded which means that
it only contains URL safe characters and does not require further encoding.

The encryption is necessary to
prevent leaking sensitive information that can be included in the `LastEvaluatedKey` provided
by DynamoDB. It also prevents a client from modifying the token and therefore manipulating the query
execution (NoSQL injection).

The length of the token depends on the length of the values for the partition and sort key of the table
or index that you are querying. The token length is at least 42 characters.

#### Returns

`undefined` \| `string`

## Methods

### [asyncIterator]

▸ **[asyncIterator]**(): `AsyncGenerator`<`T`, `void`, `void`\>

```typescript
for await (const item of items) {
  // work with item
}
```

#### Returns

`AsyncGenerator`<`T`, `void`, `void`\>

___

### all

▸ **all**(): `Promise`<`T`[]\>

Return all items from the query (up to `limit` items). This is potentially dangerous and expensive
as it this query will keep making requests to DynamoDB until there are no more items. It is recommended
to pair `all()` with a `limit()` to prevent a runaway query execution.

#### Returns

`Promise`<`T`[]\>

___

### filter

▸ **filter**<`K`\>(`predicate`): [`PaginationResponse`](#PaginationResponse)<`K`\>

Filter results by a predicate function

#### Type parameters

| Name | Type |
| :------ | :------ |
| `K` | extends `AttributeMap` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `predicate` | (`arg`: `AttributeMap`) => arg is K |

#### Returns

[`PaginationResponse`](#PaginationResponse)<`K`\>

___

### from

▸ **from**(`nextToken`): [`PaginationResponse`](#PaginationResponse)<`T`\>

Start returning results starting from `nextToken`

#### Parameters

| Name | Type |
| :------ | :------ |
| `nextToken` | `string` |

#### Returns

[`PaginationResponse`](#PaginationResponse)<`T`\>

___

### limit

▸ **limit**(`limit`): [`PaginationResponse`](#PaginationResponse)<`T`\>

Limit the number of results to `limit`. Will return at least `limit` results even when using FilterExpressions.

#### Parameters

| Name | Type |
| :------ | :------ |
| `limit` | `number` |

#### Returns

[`PaginationResponse`](#PaginationResponse)<`T`\>

___

### peek

▸ **peek**(): `Promise`<`undefined` \| `T`\>

Returns the first item in the query without advancing the iterator. `peek()` can
also be used to "prime" the iterator. It will immediately make a request to DynamoDB
and fill the iterators cache with the first page of results. This can be useful if
you have other concurrent asynchronous requests:

```typescript
const items = paginateQuery(...);

await Promise.all([
  items.peek(),
  doSomethingElse(),
]);

for await (const item of items) {
  // the first page of items has already been pre-fetched so they are available immediately
}
```

`peek` can be invoked inside a `for await` loop. `peek` returns `undefined` if there are no
more items returned or if the `limit` has been reached.

```typescript
for await (const item of items) {
  const next = await items.peek();
  if (!next) {
    // we've reached the last item
  }
}
```

`peek()` does not increment the `count` attribute.

#### Returns

`Promise`<`undefined` \| `T`\>


## PaginateQueryOptions

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | extends `AttributeMap` |

## Properties

### context

• `Optional` **context**: `string` \| `Buffer`

The context defines the additional authenticated data (AAD) that is used to generate the signature
for the pagination token. It is optional but recommended because it adds an additional layer of
authentication to the pagination token. Pagination token will be tied to the context and replaying
them in other contexts will fail. Good examples for the context are a user ID or a session ID concatenated
with the purpose of the query, such as `ListPets`. The context cannot be extracted from the pagination
token and can therefore contain sensitive data.

___

### filter

• `Optional` **filter**: (`arg`: `AttributeMap`) => arg is T

#### Type declaration

▸ (`arg`): arg is T

Filter results by a predicate function

##### Parameters

| Name | Type |
| :------ | :------ |
| `arg` | `AttributeMap` |

##### Returns

arg is T

___

### from

• `Optional` **from**: `string`

Start returning results starting from `nextToken`

___

### limit

• `Optional` **limit**: `number`

Limit the number of results to `limit`. Will return at least `limit` results even when using FilterExpressions.
