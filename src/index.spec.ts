import { strict as assert } from 'assert';
import { randomBytes, createHash } from 'crypto';
import { createTestTable, docClient } from './testing';
import { encodeKey, decodeKey, Paginator, AttributeMap, flattenKey, unflattenKey } from './index';
import { b64uDecode, sleep } from './util';

function createKey(pk: string[], sk: string[]): { PK: string, SK: string } {
    return {
        PK: pk.join(''),
        SK: sk.join(''),
    };
}

describe('aws', () => {
    describe('encodeKey', () => {
        const encKey = randomBytes(32);
        const sigKey = randomBytes(32);
        it('can encode and decode', () => {
            const key = { PK: Buffer.from('hello'), SK: Buffer.from('world') };
            const plaintext = flattenKey(key);
            const encoded = encodeKey(plaintext, { encKey, sigKey });
            const decoded = unflattenKey(decodeKey(encoded, { encKey, sigKey }));
            assert(decoded.PK.equals(key.PK));
            assert(decoded.SK.equals(key.SK));
            assert.equal(b64uDecode(encoded).length, 64);
            assert.equal(encoded.length, 86);
        });

        it('handles undefined', () => {
            const encKey = randomBytes(32);
            const key = { PK: undefined, SK: 'world' };
            const plaintext = flattenKey(key);
            const encoded = encodeKey(plaintext, { encKey, sigKey });
            const decoded = unflattenKey(decodeKey(encoded, { encKey, sigKey }));
            assert.deepEqual(decoded, { SK: 'world' });
            assert.throws(() => decodeKey(encoded, { encKey: sigKey, sigKey }), { name: 'TokenError' });
            assert.throws(() => decodeKey(encoded, { encKey, sigKey: encKey }), { name: 'TokenError' });
            assert.throws(() => decodeKey(encoded.slice(1), { encKey, sigKey }), { name: 'TokenError' });
        });
    });

    describe('paginator', () => {
        const TABLE_NAME = createTestTable('common', 'src/schema.json');

        const partitions = Array.from({ length: 10 }, () => randomBytes(16).toString('hex'));
        const partition = partitions[0];

        for (const p of partitions) {
            const items = Array.from({ length: 25 }).map((_, i) => createKey(['p:', p], ['i:', i.toString().padStart(4, '0')]));

            before(() => docClient.batchWrite({
                RequestItems: {
                    [TABLE_NAME]: items.map((i) => ({
                        PutRequest: {
                            Item: {
                                ...i,
                                GSI1PK: i.PK,
                                GSI1SK: i.SK,
                            },
                        },
                    })),
                },
            }));
        }

        const paginateQuery = Paginator.createQuery({
            client: docClient,
            key: randomBytes(32),
        });

        const paginateScan = Paginator.createScan({
            client: docClient,
            key: randomBytes(32),
        });

        it('simple case: query', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                },
            });

            const all = await paginator.all();
            assert.equal(all.length, 25);
            assert(paginator.requestCount <= 2);
            assert(paginator.finished);
            assert.equal(typeof paginator.nextToken, 'string');
        });

        it('simple case: scan', async () => {
            const paginator = paginateScan({
                TableName: TABLE_NAME,
                FilterExpression: 'begins_with(PK, :pk)',
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                },
            });

            const all = await paginator.all();
            assert.equal(all.length, 25);
            assert(paginator.requestCount <= 2);
            assert(paginator.finished);
            assert.equal(typeof paginator.nextToken, 'string');
        });

        it('low limit', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                },
                Limit: 2,
            });

            const all = await paginator.all();
            assert.equal(all.length, 25);
            assert.equal(paginator.requestCount, 13);
        });

        it('finished', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                ExpressionAttributeValues: {
                    ':pk': 'p:',
                },
            }, { limit: 10 });

            const all = await paginator.all();
            assert.equal(all.length, 10);
            assert.equal(paginator.finished, false);
            assert.equal(paginator.requestCount, 1);
        });

        it('filter expression with many empty responses', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                    ':sk': 'i:0024',
                },
                Limit: 5,
            });

            const all = await paginator.all();
            assert.equal(all.length, 1);
            assert.equal(paginator.requestCount, 5);
            assert.equal(paginator.scannedCount, 25);
        });

        it('global limit', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                    ':sk': 'i:0015',
                },
                Limit: 5,
            }, {
                limit: 2,
            });

            const all = await paginator.all();
            assert.equal(all.length, 2);
            assert.equal(paginator.requestCount, 4);
            assert.equal(paginator.scannedCount, 20);
        });

        [null, 'GSI1.1'].map((idx) => it(idx ? 'nextToken with index' : 'nextToken', async () => {
            // give index some time to update
            if (idx) await new Promise((resolve) => setTimeout(resolve, 1000));
            const paginator = ({ from, context }: { from?: string, context?: string } = {}) => paginateQuery({
                TableName: TABLE_NAME,
                ...idx ? {
                    IndexName: 'GSI1.1',
                    KeyConditionExpression: 'GSI1PK = :pk',
                    FilterExpression: 'SK >= :sk',
                } : {
                    KeyConditionExpression: 'PK = :pk',
                    FilterExpression: 'GSI1SK >= :sk',
                },
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                    ':sk': 'i:0004',
                },
                Limit: 5,
                ReturnConsumedCapacity: 'TOTAL',
            }, {
                from,
                limit: 15,
                context,
            });

            const page1 = paginator();
            assert.equal((await page1.all()).length, 15);
            assert.equal(page1.requestCount, 4);
            assert.equal(page1.consumedCapacity, 2);
            assert(page1.finished === false);

            const page2 = paginator({ from: page1.nextToken });
            assert.equal((await page2.all()).length, 6);
            assert.equal(page2.requestCount, 2);
            assert(page2.finished);
            assert.equal(page2.consumedCapacity, 1);

            const page3 = paginator({ from: page2.nextToken });
            assert.equal((await page3.all()).length, 0);
            assert.equal(page3.requestCount, 1);
            assert(page3.finished);
            assert.equal(page3.consumedCapacity, 0.5);

            const pageCtx = paginator({ from: page1.nextToken, context: 'foobar' });
            await assert.rejects(() => pageCtx.all(), { name: 'TokenError' });
        }).retries(10));

        it('predicate', async () => {
            type Foo = { PK: string; SK: string };

            // filtering by even numbers
            function isFoo(arg: AttributeMap): arg is Foo {
                return parseInt(arg.SK.slice(2) as string, 10) % 2 === 0;
            }

            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                    ':sk': 'i:0016',
                },
                Limit: 5,
            }).limit(7).filter(isFoo);
            const all = await paginator.all();
            assert.equal(all.length, 5);
            // assert.equal(paginator.requestCount, 6);
        });

        it('peek', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                    ':sk': 'i:0016',
                },
            });
            const first = await paginator.peek();
            assert.equal(first?.SK, 'i:0016');
            assert.equal(paginator.requestCount, 1);

            const again = await paginator.peek();
            assert.equal(again?.SK, 'i:0016');
            assert.equal(paginator.requestCount, 1);

            const all = await paginator.all();
            assert.equal(all.length, 9);
            assert.equal(paginator.requestCount, 1);
            assert.equal(all[0].SK, 'i:0016');

            assert.equal(await paginator.peek(), undefined);
        });

        it('nested peek', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:' + partition,
                    ':sk': 'i:0016',
                },
                Limit: 2,
            });
            let i = 16;

            await paginator.peek();
            assert.equal(paginator.requestCount, 9);

            for await (const item of paginator) {
                assert.equal(parseInt(item.SK.slice(2)), i);
                const next = await paginator.peek();
                if (i === 24) assert.equal(next, undefined);
                else assert.equal(parseInt(next?.SK.slice(2)), i + 1);
                i++;
            }
            assert.equal(paginator.requestCount, 13);
        });


        const paginateParallelScan = Paginator.createParallelScan({
            client: docClient,
            key: randomBytes(32),
        });
        it('parallel', async () => {
            const paginator = paginateParallelScan({
                TableName: TABLE_NAME,
                Limit: 10,
                FilterExpression: partitions.map((_, i) => `PK = :P${i}`).join(' OR '),
                ExpressionAttributeValues: partitions.reduce((a, p, i) => ({...a, [`:P${i}`]: `p:${p}` }), {}),
            }, { segments: 10 });

            const items: Record<string, boolean> = {};
            for await (const item of paginator) {
                items[`${item.PK}${item.SK}`] = true;
            }
            assert.equal(Object.keys(items).length, 250);
        });


        it('parallel nextToken', async () => {
            let paginator = paginateParallelScan({
                TableName: TABLE_NAME,
                Limit: 250,
                FilterExpression: partitions.map((_, i) => `PK = :P${i}`).join(' OR '),
                ExpressionAttributeValues: partitions.reduce((a, p, i) => ({...a, [`:P${i}`]: `p:${p}` }), {}),
            }, { segments: 10 });

            const items: Record<string, boolean> = {};
            let nextToken: string | undefined;
            while (!paginator.finished) {
                paginator = paginator.from(nextToken).limit(15);
                const all = await paginator.all();
                for (const item of all) items[`${item.PK}${item.SK}`] = true;
                nextToken = paginator.nextToken;
                assert(typeof nextToken === 'string');
                assert.equal(paginator.requestCount, 10);
            }
            assert.equal(Object.keys(items).length, 250);
        });

        // TODO: add tests around peek, limit and count
    });
});
