import { createDecipheriv, createCipheriv, randomBytes, CipherKey, createHmac, timingSafeEqual } from 'crypto';
import { strict as assert } from 'assert';

import { QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBDocumentClient as DynamoDBDocumentClientV3, QueryCommandInput, QueryCommandOutput, ScanCommandInput, ScanCommandOutput } from '@aws-sdk/lib-dynamodb';
import type { NativeAttributeValue } from '@aws-sdk/util-dynamodb';

import { b64uDecode, b64uEncode, uInt16Buffer } from './util';

export type AttributeMap = { [key: string]: NativeAttributeValue };

export { QueryCommandInput, QueryCommandOutput, ScanCommandInput, ScanCommandOutput } from '@aws-sdk/lib-dynamodb';

interface Keys {
    encKey: CipherKey;
    sigKey: CipherKey;
}

/**
 * @private
 * A DynamoDB key { PK: abc, SK: cdef } is encoded as follows
 * 'S' + 1 byte (length of "PK") + "PK" + 2 bytes (length of "abc") + "abc"
 * 'S' + 1 byte (length of "SK") + "SK" + 2 bytes (length of "cdef") + "cdef"
 */

export function encodeKey(key: AttributeMap, { encKey, sigKey }: Keys, aad = Buffer.alloc(0)): string {
    const parts = Object.entries(key).filter(([, v]) => v !== undefined).map(([k, v]) => [
        Buffer.from(Buffer.isBuffer(v) ? 'B' : 'S'),
        Buffer.from([k.length]), Buffer.from(k),
        uInt16Buffer(v.length), Buffer.from(v),
    ]).flat();
    const plaintext = Buffer.concat(parts);
    const iv = randomBytes(16);
    const cipher = createCipheriv('aes-256-cbc', encKey, iv);
    const cipherText = Buffer.concat([iv, cipher.update(plaintext), cipher.final()]);
    const toSign = Buffer.concat([aad, cipherText, Buffer.alloc(8)]);
    toSign.writeUint32BE(aad.length, toSign.length - 4);
    return b64uEncode(Buffer.concat([
        cipherText,
        createHmac('sha256', sigKey).update(toSign).digest().slice(0, 16),
    ]));
}

export class TokenError extends Error {
    constructor(message = 'Token is invalid') {
        super(message);
        this.name = 'TokenError';
    }
}

function _decodeKey(token: string, { encKey, sigKey }: Keys, aad = Buffer.alloc(0)): AttributeMap {
    const encrypted = b64uDecode(token);
    assert(token.length > 48 && encrypted.length % 16 === 0);
    const iv = encrypted.slice(0, 16);
    const cipherText = encrypted.slice(0, -16);
    const sig = encrypted.slice(-16);
    const toSign = Buffer.concat([aad, cipherText, Buffer.alloc(8)]);
    toSign.writeUint32BE(aad.length, toSign.length - 4);
    assert(timingSafeEqual(
        createHmac('sha256', sigKey).update(toSign).digest().slice(0, 16),
        sig,
    ));
    const decipher = createDecipheriv('aes-256-cbc', encKey, iv);
    const buf = Buffer.concat([
        decipher.update(cipherText.slice(16)),
        decipher.final(),
    ]);
    const key: AttributeMap = {};
    let pos = 0;
    while (pos < buf.length) {
        const t = buf.slice(0, 1).toString();
        pos += 1;
        const keyLen = buf[pos++];
        const valLen = buf.readUInt16BE(pos + keyLen);
        const k = buf.slice(pos, pos + keyLen).toString();
        const v = buf.slice(pos + 2 + keyLen, pos + 2 + keyLen + valLen);
        key[k] = t === 'B' ? v : v.toString();
        pos += keyLen + 2 + valLen;
    }
    return key;
}

export function decodeKey(token: string, { encKey, sigKey }: Keys, aad = Buffer.alloc(0)): AttributeMap {
    try {
        return _decodeKey(token, { encKey, sigKey }, aad);
    } catch (err) {
        throw new TokenError();
    }
}

export interface PaginationResponseOptions<T extends AttributeMap> extends PaginateQueryOptions<T>, PaginatorOptions {
    query: QueryCommandInput | ScanCommandInput;
    key: () => Promise<CipherKey>;
    method: 'scan' | 'query';
}

/**
 * ## PaginationResponse
 * 
 * The `PaginationResponse` class implements the query result iterator. It has a number of
 * utility functions such as [`peek()`](#peek) and [`all()`](#all) to simplify common usage patterns.
 * 
 * The iterator can be interrupted and resumed at any time. The iterator will stop to produce
 * items after the end of the query is reached or the provided [`limit`](#limit) parameter is exceeded.
 * 
 */

export class PaginationResponse<T = AttributeMap> {
    /** Number of items yielded */
    count: number;
    /** Number of items scanned by DynamoDB */
    scannedCount: number;
    /** Number of requests made to DynamoDB */
    requestCount: number;
    /** Total consumed capacity for query */
    consumedCapacity: number;

    private _nextKey?: AttributeMap;
    private _done: boolean;
    private _resolvedKey?: Keys;
    private _curPage: AttributeMap[];
    private _lastEvaluatedKey: AttributeMap | undefined;

    private readonly _limit;
    private readonly _filter;
    private readonly _from;
    private readonly _query;
    private readonly _context;

    private readonly key;
    private readonly client;
    private readonly schema;
    private readonly indexes;
    private readonly method;


    constructor(args: PaginationResponseOptions<T>) {
        this.consumedCapacity = 0;
        this.count = 0;
        this.scannedCount = 0;
        this.requestCount = 0;
        this._done = false;

        this._limit = args.limit ?? Infinity;
        this._filter = args.filter;
        this._from = args.from;
        this._query = args.query;
        this._context = typeof args.context === 'string' ? Buffer.from(args.context) : args.context;
        this._curPage = [];

        this.key = args.key;
        this.client = args.client;
        this.schema = args.schema ?? ['PK', 'SK'] as [string, string];
        this.indexes = args.indexes ?? PaginationResponse.defaultIndex;
        this.method = args.method;
    }

    private static defaultIndex(index: string): [string, string] {
        const idx = index.split('.', 2)[0];
        return [`${idx}PK`, `${idx}SK`];
    }

    /** Return all items from the query (up to `limit` items). This is potentially dangerous and expensive
     * as it this query will keep making requests to DynamoDB until there are no more items. It is recommended
     * to pair `all()` with a `limit()` to prevent a runaway query execution.
     */
    async all(): Promise<T[]> {
        const { _filter: filter } = this;
        const items: PredicateFor<typeof filter>[] = [];
        for await (const item of this) items.push(item);
        return items;
    }

    /** Returns the first item in the query without advancing the iterator. `peek()` can
     * also be used to "prime" the iterator. It will immediately make a request to DynamoDB
     * and fill the iterators cache with the first page of results. This can be useful if
     * you have other concurrent asynchronous requests:
     * 
     * ```typescript
     * const items = paginateQuery(...);
     * 
     * await Promise.all([
     *   items.peek(),
     *   doSomethingElse(),
     * ]);
     * 
     * for await (const item of items) {
     *   // the first page of items has already been pre-fetched so they are available immediately
     * }
     * ```
     * 
     * `peek` can be invoked inside a `for await` loop. `peek` returns `undefined` if there are no
     * more items returned or if the `limit` has been reached.
     * 
     * ```typescript
     * for await (const item of items) {
     *   const next = await items.peek();
     *   if (!next) {
     *     // we've reached the last item
     *   }
     * }
     * ```
     * 
     * `peek()` does not increment the `count` attribute.
     */
    async peek(): Promise<T | undefined> {
        for await (const item of this) {
            this._curPage.unshift(item);
            this.count -= 1;
            return item;
        }
        return undefined;
    }

    private clone<K>(args: Partial<PaginationResponseOptions<K>>): PaginationResponse<K | T> {
        const { _limit: limit, _from: from, _query: query, _filter: filter, client, key, method } = this;
        return new PaginationResponse<K | T>({
            client,
            key,
            query,
            filter,
            from,
            limit,
            method,
            ...args,
        });
    }

    /** Filter results by a predicate function */
    filter<K extends AttributeMap>(predicate: (arg: AttributeMap) => arg is K): PaginationResponse<K> {
        return this.clone({ filter: predicate }) as PaginationResponse<K>;
    }

    /** Start returning results starting from `nextToken` */
    from(nextToken: string): PaginationResponse<T> {
        return this.clone({ from: nextToken });
    }

    /** Limit the number of results to `limit`. Will return at least `limit` results even when using FilterExpressions. */
    limit(limit: number): PaginationResponse<T> {
        return this.clone({ limit });
    }

    private async ensureResolvedKey(): Promise<Keys> {
        if (!this._resolvedKey) {
            const key = await (typeof this.key === 'function' ? this.key() : this.key);
            this._resolvedKey = {
                encKey: createHmac('sha256', key).update(Buffer.from([1])).digest(),
                sigKey: createHmac('sha256', key).update(Buffer.from([2])).digest(),
            };
        }
        return this._resolvedKey;
    }

    /**
     * Token to resume query operation from the current position. The token is generated from the `LastEvaluatedKey`
     * attribute provided by DynamoDB and then AES256 encrypted such that it can safely be provided to an
     * untrustworthy client (such as a user browser or mobile app). The token is Base64 URL encoded which means that
     * it only contains URL safe characters and does not require further encoding.
     * 
     * The encryption is necessary to
     * prevent leaking sensitive information that can be included in the `LastEvaluatedKey` provided
     * by DynamoDB. It also prevents a client from modifying the token and therefore manipulating the query
     * execution (NoSQL injection).
     * 
     * The length of the token depends on the length of the values for the partition and sort key of the table
     * or index that you are querying. The token length is at least 42 characters.
     */
    get nextToken(): string | undefined {
        if (!this._nextKey) return undefined;
        if (!this._resolvedKey) throw new Error('Encryption key is not resolved yet');
        return encodeKey(this._nextKey, this._resolvedKey, this._context);
    }

    /** Returns true if all items for this query have been returned from DynamoDB. */
    get finished(): boolean {
        return this._done;
    }


    private buildLastEvaluatedKey(item: AttributeMap, index?: string): AttributeMap {
        let key: [string, string?];
        if (index === undefined) {
            key = this.schema;
        } else {
            key = typeof this.indexes === 'function' ? this.indexes(index) : this.indexes[index];
        }

        const [pk, sk] = key;

        return sk === undefined ? {
            [pk]: item[pk],
        } : {
            [pk]: item[pk],
            [sk]: item[sk],
        };
    }

    private async query(query: ScanCommandInput | QueryCommandInput): Promise<ScanCommandOutput | QueryCommandOutput> {
        if ('describeTable' in this.client) {
            throw new Error('Please provide a DynamoDB DocumentClient');
        } else if ('send' in this.client) {
            const command = this.method === 'query' ? new QueryCommand(query) : new ScanCommand(query);
            return this.client.send(command);
        } else {
            return this.client[this.method](query).promise() as Promise<QueryCommandOutput>;
        }
    }

    private async getItems(): Promise<AttributeMap[]> {
        const items = this._curPage;
        if (items.length) return items;
        if (this._done) return [];
        const [res] = await Promise.all([
            this.query({
                ...this._query,
                ExclusiveStartKey: this._nextKey,
            }),
            this.ensureResolvedKey(),
        ]);
        this.scannedCount += res.ScannedCount || 0;
        this.requestCount += 1;
        this.consumedCapacity += res.ConsumedCapacity?.CapacityUnits || 0;
        this._lastEvaluatedKey = res.LastEvaluatedKey;
        if (!this._lastEvaluatedKey) this._done = true;
        if (res.Items) items.push(...res.Items);
        return items;
    }

    /**
     * ```typescript
     * for await (const item of items) {
     *   // work with item
     * }
     * ```
     */

    async* [Symbol.asyncIterator](): AsyncGenerator<T, void, void> {
        const { _limit: limit, _from: from, _filter: filter, _query: query } = this;
        if (from && !this._nextKey) this._nextKey = decodeKey(from, await this.ensureResolvedKey(), this._context);
        loop: do {
            const items = await this.getItems();
            while (items.length) {
                const item = items.shift();
                if (!item) continue;
                this._nextKey = this.buildLastEvaluatedKey(item, query.IndexName);
                if (filter && !filter(item)) continue;
                this.count += 1;
                yield item as T;
                if (this.count === limit) break loop;
            }
            // prefer LastEvaluatedKey over our manually built key. If there are query filters involved
            // then DynamoDB might have progressed much further and the LastEvaluatedKey will not be
            // the key of the last item we saw but items that were skipped due to the filter expression.
            if (this._lastEvaluatedKey) this._nextKey = this._lastEvaluatedKey;
            else break;
        } while (this.count < limit);
    }
}

type DynamoDBDocumentClientV2 = {
    describeTable?: undefined; // ensure we have a DocumentClient
    query: (...args: any) => { promise: () => Promise<Omit<QueryCommandOutput, '$metadata'>> }; // v2
    scan: (...args: any) => { promise: () => Promise<Omit<ScanCommandOutput, '$metadata'>> }; // v2
};

/**
 * ## PaginatorOptions
 */
export interface PaginatorOptions {
    /**
     * A 32-byte encryption key (e.g. `crypto.randomBytes(32)`). The `key` parameter also
     * accepts a Promise that resolves to a key or a function that resolves to a Promise of a key.
     * 
     * If a function is passed, that function is only called once. The function is called concurrently
     * with the first query request to DynamoDB to reduce the overall latency for the first query. The
     * key is cached and the function is not called again.
     */
    key: CipherKey | Promise<CipherKey> | (() => CipherKey | Promise<CipherKey>);
    /**
     * AWS SDK v2 or v3 DynamoDB Document Client.
     */
    client: DynamoDBDocumentClientV2 | DynamoDBDocumentClientV3;
    /**
     * Names for the partition and sort key of the table. Defaults to `['PK', 'SK']`.
     */
    schema?: [partitionKey: string, sortKey?: string];
    /**
     * Object that resolves an index name to the partition and sort key for that index.
     * Also accepts a function that builds the names based on the index name.
     * 
     * Defaults to ```(index) => [`${index}PK`, `${index}PK`]```.
     */
    indexes?: Record<string, [partitionKey: string, sortKey?: string]> | ((index: string) => [partitionKey: string, sortKey?: string]);
}

/**
 * ## PaginateQueryOptions
 */
export interface PaginateQueryOptions<T extends AttributeMap> {
    /** Limit the number of results to `limit`. Will return at least `limit` results even when using FilterExpressions. */
    limit?: number;
    /** Start returning results starting from `nextToken` */
    from?: string;
    /** Filter results by a predicate function */
    filter?: (arg: AttributeMap) => arg is T;
    /**
     * The context defines the additional authenticated data (AAD) that is used to generate the signature
     * for the pagination token. It is optional but recommended because it adds an additional layer of
     * authentication to the pagination token. Pagination token will be tied to the context and replaying
     * them in other contexts will fail. Good examples for the context are a user ID or a session ID concatenated
     * with the purpose of the query, such as `ListPets`. The context cannot be extracted from the pagination
     * token and can therefore contain sensitive data.
     */
    context?: Buffer | string;
}

type PredicateFor<T> = T extends (arg: AttributeMap) => arg is infer K ? K : never;

/**
 * [![Apache License](https://img.shields.io/github/license/emdgroup/dynamodb-paginator.svg?style=flat-square)](https://github.com/emdgroup/dynamodb-paginator/blob/master/LICENSE)
 * [![Sponsored by EMD Group](https://img.shields.io/badge/sponsored%20by-emdgroup.com-ff55aa.svg?style=flat-square)](http://emdgroup.com)
 * [![Downloads](https://img.shields.io/npm/dw/@emdgroup/dynamodb-paginator.svg?style=flat-square)](https://www.npmjs.com/package/@emdgroup/dynamodb-paginator)
 * 
 * # DynamoDB Paginator
 * 
 * Features:
 *   * Supports Binary and String key types
 *   * Generates AES256 encrypted and authenticated pagination tokens
 *   * Works with TypeScript type guards natively
 *   * Ensures a minimum number of items when using a `FilterExpression`
 *   * Compatible with AWS SDK v2 and v3
 * 
 * Pagination in NoSQL stores such as DynamoDB can be challenging. This
 * library provides a developer friendly interface around the DynamoDB `Query` and `Scan` APIs.
 * It generates and encrypted and authenticated pagination token that can be shared with an untrustworthy
 * client (like the browser or a mobile app) without disclosing potentially sensitive data and protecting
 * the integrity of the token.
 * 
 * **Why is the pagination token encrypted?**
 * 
 * When researching pagination with DynamoDB, you will come across blog posts and libraries that recommend
 * to JSON-encode the `LastEvaluatedKey` attribute (or even the whole query command). **This is dangerous!**
 * 
 * The token is sent to a client which can happily decode the token, look at the values for the
 * partition and sort key and even modify the token, making the application vulnerable to NoSQL injections.
 * 
 * > "Dance like nobody is watching. Encrypt like everyone is."
 * > -- Werner Vogels
 * 
 * ## Usage
 * 
 * ```typescript
 * import { Paginator } from '@emdgroup/dynamodb-paginator';
 * import { DynamoDB } from '@aws-sdk/client-dynamodb';
 * import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';
 * 
 * import * as crypto from 'crypto';
 *
 * const client = DynamoDBDocument.from(new DynamoDB({}));
 * // persist the key in the SSM parameter store or similar
 * const key = crypto.randomBytes(32);
 * 
 * const paginateQuery = Paginator.createQuery({
 *   key,
 *   client,
 * });
 * 
 * const paginator = paginateQuery({
 *   TableName: 'mytable',
 *   KeyConditionExpression: 'PK = :pk',
 *   ExpressionAttributeValues: {
 *       ':pk': 'U#ABC',
 *   },
 * });
 * ```
 * 
 * Use `for await...of` syntax:
 * 
 * ```typescript
 * for await (const item of paginator) {
 *   // do something with item
 * 
 *   // only work on the first 50 items,
 *   // then generate a pagination token and break.
 *   if (paginator.count === 50) {
 *     console.log(paginator.nextToken);
 *     break;
 *   }
 * }
 * 
 * items.requestCount; // number of requests to DynamoDB
 * ```
 * 
 * Use `await all()` syntax:
 * 
 * ```typescript
 * const items = await paginator.limit(50).all();
 * paginator.nextToken;
 * 
 * const nextPaginator = paginator.from(paginator.nextToken);
 * nextPaginator.all(); // up to 50 more items
 * ```
 * 
 * Use TypeScript guards to filter for items:
 * 
 * ```typescript
 * interface User {
 *   PK: string;
 *   SK: string;
 * }
 * 
 * function isUser(arg: Record<string, unknown>): args is User {
 *   return typeof arg.PK === 'string' && 
 *     typeof arg.SK === 'string' &&
 *     arg.PK.startsWith('U#');
 * }
 * 
 * for await (const user of paginator.filter(isUser)) {
 *   // user is of type User
 * }
 * ```
 * 
 * 
 * ## Paginator
 * 
 * The `Paginator` class is a factory for the [`PaginationResponse`](#PaginationResponse) object. This class
 * is instantiated with the 32-byte encryption key and the DynamoDB document client (versions
 * 2 and 3 of the AWS SDK are supported).
 * 
 * ```typescript
 * const paginateQuery = Paginator.createQuery({
 *   key: () => Promise.resolve(crypto.randomBytes(32)),
 *   client: documentClient,
 * });
 * ```
 * 
 * To create a paginator over a scan operation, use `createScan`.
 * 
 * ```typescript
 * const paginateScan = Paginator.createScan({
 *   key: () => Promise.resolve(crypto.randomBytes(32)),
 *   client: documentClient,
 * });
 * ```
 */

export class Paginator {
    readonly key;
    readonly client;
    readonly schema;
    readonly indexes;

    private _resolvedKey: CipherKey | undefined;

    /**
     * Use the static factory function [`create()`](#create) instead of the constructor.
     */

    constructor(args: PaginatorOptions) {
        this.key = args.key;
        this.client = args.client;
        this.schema = args.schema;
        this.indexes = args.indexes;
    }

    private async ensureResolvedKey(): Promise<CipherKey> {
        if (!this._resolvedKey) this._resolvedKey = await (typeof this.key === 'function' ? this.key() : this.key);
        return this._resolvedKey;
    }

    /**
     * Returns a function that accepts a DynamoDB Query command and return an instance of `PaginationResponse`.
     */

    public static createQuery(args: PaginatorOptions): <T extends AttributeMap>(query: QueryCommandInput, opts?: PaginateQueryOptions<T>) => PaginationResponse<T> {
        const instance = new Paginator(args);
        return instance.paginateQuery.bind(instance);
    }

    /**
     * Returns a function that accepts a DynamoDB Scan command and return an instance of `PaginationResponse`.
     */
    public static createScan(args: PaginatorOptions): <T extends AttributeMap>(scan: ScanCommandInput, opts?: PaginateQueryOptions<T>) => PaginationResponse<T> {
        const instance = new Paginator(args);
        return instance.paginateScan.bind(instance);
    }

    paginateQuery<T extends AttributeMap>(query: QueryCommandInput, { filter, ...args }: PaginateQueryOptions<T> = {}): PaginationResponse<PredicateFor<typeof filter>> {
        return new PaginationResponse({
            ...args,
            client: this.client,
            key: () => this.ensureResolvedKey(),
            schema: this.schema,
            indexes: this.indexes,
            filter,
            query,
            method: 'query',
        });
    }

    paginateScan<T extends AttributeMap>(query: ScanCommandInput, { filter, ...args }: PaginateQueryOptions<T> = {}): PaginationResponse<PredicateFor<typeof filter>> {
        return new PaginationResponse({
            ...args,
            client: this.client,
            key: () => this.ensureResolvedKey(),
            schema: this.schema,
            indexes: this.indexes,
            filter,
            query,
            method: 'scan',
        });
    }
}
