import { randomBytes } from 'crypto';

import { strict as assert } from 'assert';

export { assert };

export function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

export function b64uEncode(input: Buffer): string {
    return input.toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

export function b64uDecode(input: string): Buffer {
    return Buffer.from(input.replace(/-/g, '+').replace(/_/g, '/'), 'base64');
}

export function uInt16Buffer(value: number): Buffer {
    const buf = Buffer.alloc(2);
    buf.writeUInt16BE(value);
    return buf;
}

export function createCode(size = 16): Buffer {
    return randomBytes(size);
}
