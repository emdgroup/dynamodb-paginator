import { readFileSync, writeFileSync, readdirSync } from 'fs';

function run() {
    const readme: string[] = [];
    for (const file of [
        'QueryPaginator.md',
        'QueryPaginatorOptions.md',
        'PaginationResponse.md',
        'PaginateQueryOptions.md',
    ]) {
        const md = readFileSync(`./docs/${file}`).toString().split('\n');
        readme.push(md.slice(2).join('\n').replace(/\(..\/wiki\/(\w+)\)/g, '(#$1)'));
    }
    writeFileSync('README.md', readme.join('\n\n'));
}

run();
