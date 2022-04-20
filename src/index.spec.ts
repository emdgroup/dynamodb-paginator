import { expect } from 'chai';
import { randomBytes } from 'crypto';
import { createTestTable } from './testing';
import { encodeKey, decodeKey, QueryPaginator, AttributeMap } from '.';
import { docClient } from './testing';
import { b64uDecode } from './util';

function createKey(pk: string[], sk: string[]): { PK: string, SK: string } {
    return {
        PK: pk.join(''),
        SK: sk.join(''),
    };
}

describe('aws', () => {
    describe('encodeKey', () => {
        it('can encode and decode', () => {
            const encKey = randomBytes(32);
            const key = { PK: Buffer.from('hello'), SK: Buffer.from('world') };
            const encoded = encodeKey(key, encKey);
            const decoded = decodeKey(encoded, encKey);
            expect(decoded.PK.equals(key.PK)).to.be.true;
            expect(decoded.SK.equals(key.SK)).to.be.true;
            expect(b64uDecode(encoded)).of.length(48);
            expect(encoded).of.length(64);
        });

        it('handles undefined', () => {
            const encKey = randomBytes(32);
            const key = { PK: undefined, SK: 'world' };
            const encoded = encodeKey(key, encKey);
            const decoded = decodeKey(encoded, encKey);
            expect(decoded).to.deep.equal({
                SK: 'world'
            });
        });
    });

    describe('paginator', () => {
        const TABLE_NAME = createTestTable('common', 'src/schema.json');

        const items = Array.from({ length: 25 }).map((_, i) => createKey(['p:'], ['i:', i.toString().padStart(4, '0')]));

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

        const paginateQuery = QueryPaginator.create({
            client: docClient,
            key: randomBytes(32),
        });

        const paginateScan = QueryPaginator.createScan({
            client: docClient,
            key: randomBytes(32),
        });

        it('simple case: query', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                ExpressionAttributeValues: {
                    ':pk': 'p:',
                },
            });

            const all = await paginator.all();
            expect(all.length).to.equal(25);
            expect(paginator.requestCount).to.equal(1);
            expect(paginator.nextToken).to.be.undefined;
        });

        it('simple case: scan', async () => {
            const paginator = paginateScan({
                TableName: TABLE_NAME,
            });

            const all = await paginator.all();
            expect(all.length).to.equal(25);
            expect(paginator.requestCount).to.equal(1);
            expect(paginator.nextToken).to.be.undefined;
        });

        it('low limit', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                ExpressionAttributeValues: {
                    ':pk': 'p:',
                },
                Limit: 2,
            });

            const all = await paginator.all();
            expect(all.length).to.equal(25);
            expect(paginator.requestCount).to.equal(13);
        });

        it('filter expression with many empty responses', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:',
                    ':sk': items[24].SK,
                },
                Limit: 5,
            });

            const all = await paginator.all();
            expect(all.length).to.equal(1);
            expect(paginator.requestCount).to.equal(6);
            expect(paginator.scannedCount).to.equal(25);
        });

        it('global limit', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:',
                    ':sk': items[16].SK,
                },
                Limit: 5,
            }, {
                limit: 2,
            });

            const all = await paginator.all();
            expect(all.length).to.equal(2);
            expect(paginator.requestCount).to.equal(4);
            expect(paginator.scannedCount).to.equal(20);
        });

        it('nextToken', async () => {
            const paginator = (nextToken?: string) => paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:',
                    ':sk': items[4].SK,
                },
                Limit: 5,
                ReturnConsumedCapacity: 'TOTAL',
            }, {
                from: nextToken,
                limit: 15,
            });

            const page1 = paginator();
            expect((await page1.all()).length).to.equal(15);
            expect(page1.requestCount).to.equal(4);
            expect(page1.consumedCapacity).to.equal(2);
            expect(page1.nextToken).to.be.a('string').of.length(64);

            const page2 = paginator(page1.nextToken);
            expect((await page2.all()).length).to.equal(6);
            expect(page2.requestCount).to.equal(2);
            expect(page2.consumedCapacity).to.equal(1);
        });

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
                    ':pk': 'p:',
                    ':sk': items[16].SK,
                },
                Limit: 5,
            }).limit(7).filter(isFoo);
            const all = await paginator.all();
            expect(all.length).to.equal(5);
            expect(paginator.requestCount).to.equal(6);
        });

        it('peek', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:',
                    ':sk': items[16].SK,
                },
            });
            const first = await paginator.peek();
            expect(first?.SK).to.equal('i:0016');
            expect(paginator.requestCount).to.equal(1);

            const again = await paginator.peek();
            expect(again?.SK).to.equal('i:0016');
            expect(paginator.requestCount).to.equal(1);

            const all = await paginator.all();
            expect(all.length).to.equal(9);
            expect(paginator.requestCount).to.equal(1);
            expect(all[0].SK).to.equal('i:0016');

            expect(await paginator.peek()).to.be.undefined;
        });

        it('nested peek', async () => {
            const paginator = paginateQuery({
                TableName: TABLE_NAME,
                KeyConditionExpression: 'PK = :pk',
                FilterExpression: 'GSI1SK >= :sk',
                ExpressionAttributeValues: {
                    ':pk': 'p:',
                    ':sk': items[16].SK,
                },
                Limit: 2,
            });
            let i = 16;

            await paginator.peek();
            expect(paginator.requestCount).to.equal(9);

            for await (const item of paginator) {
                expect(parseInt(item.SK.slice(2))).to.equal(i);
                const next = await paginator.peek();
                if (i === 24) expect(next).to.be.undefined;
                else expect(parseInt(next?.SK.slice(2))).to.equal(i + 1);
                i++;
            }
            expect(paginator.requestCount).to.equal(13);
        });

        // TODO: add tests around peek, limit and count
    });
});
