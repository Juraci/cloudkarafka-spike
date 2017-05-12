const Kafka = require('node-rdkafka');
const TOPIC_NAME = '5nik-darksouls';
const fs = require('fs');
const brokers = 'steamer-01.srvs.cloudkafka.com:9093,steamer-02.srvs.cloudkafka.com:9093,steamer-03.srvs.cloudkafka.com:9093';

fs.writeFileSync("/tmp/kafka.ca", process.env.CLOUDKARAFKA_CA);
fs.writeFileSync("/tmp/kafka.cert", process.env.CLOUDKARAFKA_CERT);
fs.writeFileSync("/tmp/kafka.key", process.env.CLOUDKARAFKA_PRIVATE_KEY);

console.log('>>>> ca', process.env.CLOUDKARAFKA_CA);
console.log('>>>> cert', process.env.CLOUDKARAFKA_CERT);
console.log('>>>> private key', process.env.CLOUDKARAFKA_PRIVATE_KEY);

const config = {
  'client.id': 'example-node-kafka',
  'group.id': 'squad-a',
  'socket.keepalive.enable': true,
  'enable.auto.commit': true,
  'ssl.ca.location': '/tmp/kafka.ca',
  'ssl.certificate.location': '/tmp/kafka.cert',
  'ssl.key.location': '/tmp/kafka.key',
  'security.protocol': 'ssl',
  'metadata.broker.list': brokers.split(',')
};

const consumer = new Kafka.KafkaConsumer(config, {
  'auto.offset.reset': 'beginning'
});

consumer.on('event.log', log => console.log(log))

consumer.on('error', (err) => {
  console.error('Error from consumer');
  console.error(err);
});

consumer.on('ready', (arg) => {
  console.log('consumer ready.', JSON.stringify(arg));
  consumer.subscribe([TOPIC_NAME]);
  consumer.consume();
});

consumer.on('data', (m) => {
  console.log('[CONSUMER 1]');
  console.log(JSON.stringify(m));
  console.log(m.value.toString());
});

consumer.on('disconnected', (arg) => {
  console.log('consumer disconnected. ' + JSON.stringify(arg));
});

consumer.connect({}, err => console.log(err));
