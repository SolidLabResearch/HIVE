import {fork, ChildProcess} from 'child_process';
import * as path from 'path';

/**
 * Starts the Approximation Approach Orchestrator as a child process.
 */
export async function startApproximationApproach() : Promise<void> {
            const childPath = path.resolve(__dirname, '../ApproximationApproachOrchestrator.js');

    const child: ChildProcess = fork(childPath);
    
    child.on('message', (message) => {
        if (message === 'ready') {
            console.log('Child process is ready to approximate queries.');
        } else {
            console.log('Received message from child:', message);
        }
    });

    child.on('error', (error) => {
        console.error('Error in child process:', error);
    });

    child.on('exit', (code) => {
        if (code !== 0) {
            console.error(`Child process exited with code ${code}`);
        } else {
            console.log('Child process exited successfully.');
        }
    });
}


startApproximationApproach();