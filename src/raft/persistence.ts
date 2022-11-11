import { Entry } from './log';
import * as fs from 'fs';

// Uses blocking file operations, which is not performant, but necessary for safety of Raft.
// Overwrites the whole file for simplicity. An improvement would be to overwrite only the necessary parts of the file.

export type PersistenceFile<LogValueType> = {
    entries: Array<Entry<LogValueType>>;
    votedFor: number | undefined;
    currentTerm: number;
};

export function writePersistenceFile<LogValueType>(
    filePath: string,
    file: PersistenceFile<LogValueType>,
) {
    const fileContentsString = JSON.stringify(file);
    const fileContents = Buffer.from(fileContentsString, 'utf-8');
    fs.writeFileSync(filePath, fileContents);
}

export function readPersistenceFile<LogValueType>(
    filePath: string,
): PersistenceFile<LogValueType> {
    try {
        const fileContents = fs.readFileSync(filePath);
        const fileContentsString = fileContents.toString('utf-8');
        return JSON.parse(fileContentsString);
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
        if (error.code === 'ENOENT') {
            return {
                entries: [],
                currentTerm: 0,
                votedFor: undefined,
            };
        }
        throw error;
    }
}
