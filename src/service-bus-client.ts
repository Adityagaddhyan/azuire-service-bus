import {
  ServiceBusAdministrationClient,
  ServiceBusClient,
  ServiceBusMessage,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
  ServiceBusSender,
} from '@azure/service-bus';

export default class ServicBusClient {
  private connectionString: string;
  private serviceBusAdministrationClient: ServiceBusAdministrationClient;
  private serviceBusClient: ServiceBusClient;
  private senders: { [key: string]: ServiceBusSender } = {};
  private receivers: { [key: string]: ServiceBusReceiver } = {};
  private queueNames: Set<string>;

  constructor() {
    this.connectionString = process.env.AzureASBEndpoint;
    this.serviceBusAdministrationClient = new ServiceBusAdministrationClient(
      this.connectionString
    );
    this.serviceBusClient = new ServiceBusClient(this.connectionString, {
      retryOptions: { maxRetries: 5 },
    });
    this.queueNames = new Set();
  }

  public async createQueue(
    queueName,
    requiresDuplicateDetection = true,
    duplicateDetectionHistoryTimeWindow = 'P7D'
  ) {
    this.queueNames.add(queueName);
    if (!(await this.serviceBusAdministrationClient.queueExists(queueName))) {
      console.log('CREATING new queue with name', queueName);
      await this.serviceBusAdministrationClient.createQueue(queueName, {
        deadLetteringOnMessageExpiration: true,
        requiresDuplicateDetection: requiresDuplicateDetection,
        duplicateDetectionHistoryTimeWindow:
          duplicateDetectionHistoryTimeWindow,
      });
    }
    if (!this.senders[queueName])
      this.senders[queueName] = await this.serviceBusClient.createSender(
        queueName
      );
    if (!this.receivers[queueName])
      this.receivers[queueName] = await this.serviceBusClient.createReceiver(
        queueName,
        { receiveMode: 'peekLock' }
      );
    console.log(`${queueName} created successfully`);
  }

  public async sendMessage(message: ServiceBusMessage, queueName: string) {
    console.log(`Sending message ${JSON.stringify(message.body)}`);
    return await this.senders[queueName].sendMessages(message);
  }
  public async subscribe(queueName: string, callback) {
    const messageHandler = async (recievedMessage) => {
      return callback(recievedMessage);
    };
    this.receivers[queueName].subscribe(
      {
        processError: this.defaultErrorHandler,
        processMessage: messageHandler,
      },
      {
        autoCompleteMessages: false,
      }
    );
  }

  public async completeMessage(message: ServiceBusReceivedMessage, queueName: string) {
    return await this.receivers[queueName].completeMessage(message);
  }

  public async scheduleMessage(
    message: ServiceBusMessage,
    queueName: string,
    timeStamp: Date
  ) {
    return await this.senders[queueName].scheduleMessages(message, timeStamp);
  }

  public async subscribeDeadLetterQueue(queueName: string, callback) {
    const messageHandler = async (recievedMessage) => {
      return callback(recievedMessage);
    };
    return await this.serviceBusClient
      .createReceiver(queueName, { subQueueType: 'deadLetter' })
      .subscribe(
        {
          processError: this.defaultErrorHandler,
          processMessage: messageHandler,
        },
        {}
      );
  }

  async defaultErrorHandler(error) {
    console.error('UNABLE TO RECEIVE MESSAGE. ERROR : ', error);
    throw error;
  }
}
