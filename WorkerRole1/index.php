<?php
    include_once('vendor/autoload.php');

    use WindowsAzure\Common\ServicesBuilder;
    use WindowsAzure\Common\ServiceException;
    use WindowsAzure\Queue\Models\CreateQueueOptions;
    use WindowsAzure\Queue\Models\ListMessagesOptions;
    use WindowsAzure\Queue\Models\ListQueuesOptions;
    use WindowsAzure\Queue\Models\PeekMessagesOptions;
    use WindowsAzure\Table\Models\EdmType;
    use WindowsAzure\Table\Models\Entity;

    //init queue and tables

class Worker {

    /**
     * @var \WindowsAzure\Queue\QueueRestProxy queueProxy
     */
    private $queueProxy;

    /**
     * @var \WindowsAzure\Table\TableRestProxy tableProxy
     */
    private $tableProxy;

    public function __construct()
    {
        $connectionString = 'DefaultEndpointsProtocol=http;'
                           . 'AccountName=exampleproject;'
                           . 'AccountKey=we3GAzMMS0w2fXn0MI42OlGai5oaBoJYRm8MWPEE2yao0rzyCEucrhwDaRnlEtGxKPgEkkIgmwmZtYxkcnN4Xw==';
        $this->queueProxy = ServicesBuilder::getInstance()->createQueueService($connectionString);

        $this->initQueue();

        $this->tableProxy = ServicesBuilder::getInstance()->createTableService($connectionString);

        $this->initTable();
    }

    private function initQueue()
    {
        $queuesOptions = new ListQueuesOptions();
        $queuesOptions->setPrefix('test-queue');
        $queues = $this->queueProxy->listQueues($queuesOptions)->getQueues();
        if (count($queues) == 0) {
            $this->queueProxy->createQueue('test-queue');
        }

    }

    private function initTable()
    {
        $existingTables = $this->tableProxy->queryTables('config')->getTables();
        if (count($existingTables) == 0) {
            $this->tableProxy->createTable("config");
        }
    }

    private function getTimeout()
    {
        try {
            $timeout = $this->tableProxy
                        ->getEntity("config", 'workerConfig', 'timeout')
                        ->getEntity()
                        ->getPropertyValue('value');
        } catch(\Exception $e) {
            $entity = new Entity();
            $entity->setPartitionKey("workerConfig");
            $entity->setRowKey('timeout');
            $entity->addProperty("value", EdmType::INT32, 10);
            $this->tableProxy->insertEntity("config", $entity);
            $timeout = 10;
        }
        return $timeout;
    }

    private function getCount()
    {
        try {
            $count = $this->tableProxy
                                ->getEntity("config", 'workerConfig', 'count')
                                ->getEntity()
                                ->getPropertyValue('value');
        } catch (\Exception $e) {
            $entity = new Entity();
            $entity->setPartitionKey("workerConfig");
            $entity->setRowKey('count');
            $entity->addProperty("value", EdmType::INT32, 1);
            $this->tableProxy->insertEntity("config", $entity);
            $count = 1;
        }
        return $count;
    }

    public function run()
    {
        while (true) {
            //Our worker works like a daemon
            $timeout = $this->getTimeout();
            $count = $this->getCount();

            $options = new ListMessagesOptions();
            $options->setNumberOfMessages($count);

            $listMessagesResult = $this->queueProxy->listMessages('test-queue', $options);
            $locked_messages = $listMessagesResult->getQueueMessages();
            sleep($timeout);
            if (count($locked_messages)) {
                foreach ($locked_messages as $message) {
                    $messageId = $message->getMessageId();
                    $popReceipt = $message->getPopReceipt();
                    $this->queueProxy->deleteMessage('test-queue', $messageId, $popReceipt);
                }
            }
        }
    }
}

    $worker = new Worker();
    $worker->run();

?>