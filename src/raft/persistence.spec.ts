import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { Entry } from './log';
import {
    PersistenceFile,
    readPersistenceFile,
    writePersistenceFile,
} from './persistence';

describe('persistence', () => {
    let tmpDir = '';
    let filePath = '';

    beforeEach(async () => {
        tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'persistence'));
        filePath = path.join(tmpDir, 'log');
    });

    it('writes and reads from disk', () => {
        const entries: Entry<string>[] = [
            {
                type: 'noop',
                term: 0,
            },
            {
                type: 'value',
                value: 'x <- 2',
                term: 0,
                id: {
                    requestSerial: 0,
                    clientId: 1,
                },
            },
        ];
        const file: PersistenceFile<string> = {
            votedFor: 2,
            currentTerm: 23,
            entries,
        };

        writePersistenceFile(filePath, file);

        expect(readPersistenceFile(filePath)).toEqual(file);
    });
});
