import { DynamoDB, CreateTableCommandInput } from '@aws-sdk/client-dynamodb';
import { SdkError } from '@aws-sdk/smithy-client';
import { randomBytes } from 'crypto';
import { promises as fs } from 'fs';
import { sleep } from './util';
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb';

export const ddb = new DynamoDB({});
export const docClient = DynamoDBDocument.from(ddb);

export function createTestTable(
    /** Prefix for table name */
    prefix: string,
    /** File name to JSON encoded table schema or inline table schema (without TableName) */
    params: Omit<CreateTableCommandInput, 'TableName'> | string,
): string {
    let tableExists = false;
    const table = process.env.TABLE_NAME || `test-${prefix}-` + randomBytes(16).toString('hex');
    process.env.TABLE_NAME = table;

    before(async () => {
        const createTable = typeof params === 'string'
            ? JSON.parse((await fs.readFile(params)).toString()) as CreateTableCommandInput
            : params;
        await ddb.createTable({
            TableName: table,
            ...createTable,
        }).catch((err: SdkError) => {
            console.log(err.name);
            if (err.name !== 'ResourceInUseException') throw err;
            tableExists = true;
        });

        while (true) {
            const tbl = await ddb.describeTable({ TableName: table });
            const indices = tbl.Table?.GlobalSecondaryIndexes?.some((i) => i.IndexStatus !== 'ACTIVE');
            if (indices === false && tbl.Table?.TableStatus === 'ACTIVE') break;
            await sleep(250);
        }
    });

    after(async () => {
        if (!tableExists) await ddb.deleteTable({ TableName: table });
    });

    return table;
}
