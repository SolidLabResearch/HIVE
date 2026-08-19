const mqtt = require('mqtt');
const client = mqtt.connect('mqtt://localhost:1883');

client.on('connect', () => {
  client.subscribe('#');
});

client.on('message', (topic, message) => {
  if (topic === 'wearableX' || topic === 'smartphoneX') return;
  console.log('Topic:', topic, 'Message:', message.toString());
});
