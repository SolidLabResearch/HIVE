import {fork, ChildProcess} from 'child_process';
import * as path from 'path';

/**
 * Starts the Fetching Data Client Side Orchestrator as a child process.
 */
export async function startFetchingDataClientSide() : Promise<void> {
        const childPath = path.resolve(__dirname, '../FetchingAllDataClientSide.js');

    const child: ChildProcess = fork(childPath);

    child.on('message', (message) => {
        if (message === 'ready') {
            console.log('Child process is ready to fetch data.');
        } else {
            console.log('Received message from child:', message);
        }
    });
    child.on('error', (error) => {
        console.error('Error in child process:', error);
    }
    );
    child.on('exit', (code) => {
        if (code !== 0) {
            console.error(`Child process exited with code ${code}`);
        } else {
            console.log('Child process exited successfully.');
        }
    }
    );
}

startFetchingDataClientSide();

