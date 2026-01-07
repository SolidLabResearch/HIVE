const { spawn } = require('child_process');
const fs = require('fs');

console.log("Starting debug test...");
const logStream = fs.createWriteStream('debug_output.log');

// Use empty DATA_PATH to rely on defaults (or specific path if needed)
const env = { ...process.env, DATA_PATH: '' };

console.log("Spawning Orchestrator...");
const orchestrator = spawn('node', ['dist/approaches/StreamingQueryChunkedApproachOrchestrator.js'], {
  stdio: ['ignore', 'pipe', 'pipe'],
  env
});

orchestrator.stdout.pipe(logStream);
orchestrator.stderr.pipe(logStream);

setTimeout(() => {
  console.log("Spawning Publisher...");
  const publisher = spawn('node', ['dist/streamer/src/publish.js'], {
    stdio: ['ignore', 'pipe', 'pipe'],
    env
  });
  publisher.stdout.pipe(logStream);
  publisher.stderr.pipe(logStream);

  setTimeout(() => {
    console.log("Stopping processes...");
    publisher.kill();
    orchestrator.kill();
    logStream.end();
  }, 45000); // Run for 45s to get at least one window output
}, 2000);
