import { ServiceBusReceivedMessage } from '@azure/service-bus';
import express from 'express';
import ServicBusClient from './service-bus-client';
import { faker } from '@faker-js/faker';
import moment from 'moment';
require('dotenv').config();
const app = express();
const port = 3010;
const bodyParser = require('body-parser');
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
const servicBusClient = new ServicBusClient();
const queueName = 'testQueue';
async function main() {
  const callback = (message: ServiceBusReceivedMessage) => {
    console.log('MESSAGE RECEIVED : ', message.body);
    servicBusClient.completeMessage(message, queueName);
  };
  await servicBusClient.createQueue(queueName);
  await servicBusClient.subscribe(queueName, callback);
}
main();

app.get('/', (req, res) => {
  res.send('Hello World!');
});
app.post('/sendMessage', async (req, res) => {
  const x = await servicBusClient.sendMessage(req.body.message, queueName);
  res.send(x);
});
app.post('/schedule', async (req, res) => {
  const x = [];
  for (let i = 0; i < req.body.count; i++) {
    let name = faker.name.findName();
    let id = faker.datatype.number();
    const timestamp = moment()
      .add(2 * i, 'seconds')
      .toDate();
    x.push(
      servicBusClient.scheduleMessage(
        { body: name, messageId: id },
        queueName,
        timestamp
      )
    );
    console.log('SENDING : ', name, id, timestamp);
  }
  res.send(await Promise.all(x));
});
app.listen(port, () => {
  return console.log(`Express is listening at http://localhost:${port}`);
});
