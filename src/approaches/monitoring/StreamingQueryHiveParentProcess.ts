import { fork, ChildProcess } from 'child_process';
import * as path from 'path';

/**
 * Starts the Streaming Query Hive Orchestrator as a child process.
 */
export async function startStreamingHiveApproach(): Promise<void> {
    const childPath = path.resolve(__dirname, '../StreamingQueryHiveApproachOrchestrator.js');
    const child: ChildProcess = fork(childPath);

    child.on('message', (message) => {
        if (message === 'ready') {
            console.log('Child process is ready to stream Hive queries.');
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

startStreamingHiveApproach();