import { Entry } from './log';
import * as fs from 'fs';

// Uses blocking file operations, which is not performant, but necessary for safety of Raft.
// Overwrites the whole file for simplicity. An improvement would be to overwrite only the necessary parts of the file.

export function writeEntries<LogValueType>(
    filePath: string,
    entries: ReadonlyArray<Entry<LogValueType>>,
) {
    const fileContentsString = JSON.stringify(entries);
    const fileContents = Buffer.from(fileContentsString, 'utf-8');
    fs.writeFileSync(filePath, fileContents);
}

export function readEntries<LogValueType>(
    filePath: string,
): Entry<LogValueType>[] {
    try {
        const fileContents = fs.readFileSync(filePath);
        const fileContentsString = fileContents.toString('utf-8');
        return JSON.parse(fileContentsString);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
        if (error.code === 'ENOENT') {
            return [];
        }
        throw error;
    }
}
