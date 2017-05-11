const Kafka = require('node-rdkafka');
const TOPIC_NAME = '5nik-darksouls';
const messages = process.argv.slice(2);

const producer = new Kafka.Producer({
  'client.id': 'example-node-kafka',
  'metadata.broker.list': 'localhost:9092'
});

//logging all errors
producer.on('error', (err) => {
  console.error('Error from producer');
  console.error(err);
});

producer.on('ready', (arg) => {
  console.log('producer ready.' + JSON.stringify(arg));
  const topic = producer.Topic(TOPIC_NAME);
  const partition = -1;
  console.time('producing');
  for(let message of messages) {
    producer.produce(topic, partition, new Buffer(message));
  }
  console.timeEnd('producing');
});

producer.on('disconnected', function(arg) {
  console.log('producer disconnected. ' + JSON.stringify(arg));
});

producer.connect();
