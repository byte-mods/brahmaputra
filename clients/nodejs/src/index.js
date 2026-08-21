'use strict';

/**
 * Brahmaputra client for Node.js.
 *
 *   const { Producer, GroupConsumer } = require('brahmaputra');
 *
 *   const producer = await Producer.connect('127.0.0.1', 9092);
 *   await producer.send('orders', Buffer.from('{"id":1}'), { key: Buffer.from('user-7') });
 *   await producer.flush();
 *
 *   const consumer = await GroupConsumer.connect('127.0.0.1', 9092, 'billing');
 *   consumer.subscribe(['orders']);
 *   for (;;) {
 *     for (const record of await consumer.poll(500)) handle(record.value);
 *     await consumer.commit();   // at-least-once: commit after processing
 *   }
 */

const protocol = require('./protocol');
const client = require('./client');
const group = require('./group');

module.exports = {
  ...protocol,
  ...client,
  ...group,
};
