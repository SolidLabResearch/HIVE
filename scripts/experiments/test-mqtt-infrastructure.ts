#!/usr/bin/env ts-node

/**
 * MQTT Infrastructure Test
 * 
 * This script tests the MQTT broker connectivity and basic pub/sub functionality
 * required for the streaming query experiments.
 */

import * as mqtt from 'mqtt';

class MQTTInfrastructureTest {
    private readonly brokerUrl = 'mqtt://localhost:1883';
    
    public async testMQTTInfrastructure(): Promise<void> {
        console.log('🔌 Testing MQTT Infrastructure');
        console.log('=' .repeat(40));
        
        try {
            // Test 1: Basic connectivity
            console.log('1️⃣  Testing MQTT broker connectivity...');
            await this.testConnectivity();
            
            // Test 2: Pub/Sub functionality
            console.log('\n2️⃣  Testing publish/subscribe functionality...');
            await this.testPubSub();
            
            console.log('\n✅ MQTT infrastructure test completed successfully!');
            console.log('\n📋 Next steps:');
            console.log('• MQTT broker is running and accessible');
            console.log('• Ready to run real-world experiments: npm run experiment:run-realworld');
            
        } catch (error) {
            console.error('\n❌ MQTT infrastructure test failed:', error);
            console.log('\n🔧 Troubleshooting:');
            console.log('• Install and start MQTT broker: brew install mosquitto && brew services start mosquitto');
            console.log('• Or use Docker: docker run -it -p 1883:1883 eclipse-mosquitto');
            console.log('• Check if port 1883 is available: lsof -i :1883');
            process.exit(1);
        }
    }
    
    /**
     * Test basic MQTT connectivity
     */
    private async testConnectivity(): Promise<void> {
        return new Promise((resolve, reject) => {
            const client = mqtt.connect(this.brokerUrl, {
                connectTimeout: 5000,
                reconnectPeriod: 0
            });
            
            const timeout = setTimeout(() => {
                client.end();
                reject(new Error('Connection timeout - MQTT broker may not be running'));
            }, 6000);
            
            client.on('connect', () => {
                clearTimeout(timeout);
                console.log('  ✅ Successfully connected to MQTT broker');
                client.end();
                resolve();
            });
            
            client.on('error', (error) => {
                clearTimeout(timeout);
                client.end();
                reject(error);
            });
        });
    }
    
    /**
     * Test publish/subscribe functionality
     */
    private async testPubSub(): Promise<void> {
        return new Promise((resolve, reject) => {
            const testTopic = 'test/frequency-experiment';
            const testMessage = JSON.stringify({
                test: true,
                timestamp: new Date().toISOString(),
                data: 'MQTT test message'
            });
            
            let messageReceived = false;
            
            // Create subscriber
            const subscriber = mqtt.connect(this.brokerUrl);
            
            // Create publisher  
            const publisher = mqtt.connect(this.brokerUrl);
            
            const timeout = setTimeout(() => {
                subscriber.end();
                publisher.end();
                if (!messageReceived) {
                    reject(new Error('Message not received within timeout'));
                }
            }, 5000);
            
            subscriber.on('connect', () => {
                subscriber.subscribe(testTopic, (err) => {
                    if (err) {
                        clearTimeout(timeout);
                        subscriber.end();
                        publisher.end();
                        reject(err);
                    }
                });
            });
            
            subscriber.on('message', (topic, message) => {
                if (topic === testTopic) {
                    try {
                        const receivedData = JSON.parse(message.toString());
                        if (receivedData.test === true) {
                            messageReceived = true;
                            clearTimeout(timeout);
                            console.log('  ✅ Successfully published and received test message');
                            subscriber.end();
                            publisher.end();
                            resolve();
                        }
                    } catch (error) {
                        clearTimeout(timeout);
                        subscriber.end();
                        publisher.end();
                        reject(new Error('Received invalid message format'));
                    }
                }
            });
            
            publisher.on('connect', () => {
                // Give subscriber time to subscribe
                setTimeout(() => {
                    publisher.publish(testTopic, testMessage, (err) => {
                        if (err) {
                            clearTimeout(timeout);
                            subscriber.end();
                            publisher.end();
                            reject(err);
                        }
                    });
                }, 100);
            });
            
            subscriber.on('error', (error) => {
                clearTimeout(timeout);
                subscriber.end();
                publisher.end();
                reject(error);
            });
            
            publisher.on('error', (error) => {
                clearTimeout(timeout);
                subscriber.end();
                publisher.end();
                reject(error);
            });
        });
    }
}

// Run the test if this file is executed directly
if (require.main === module) {
    const tester = new MQTTInfrastructureTest();
    tester.testMQTTInfrastructure().catch(error => {
        console.error('MQTT test failed:', error);
        process.exit(1);
    });
}

export { MQTTInfrastructureTest };
