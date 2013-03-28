<?php
/**
 * Created by JetBrains PhpStorm.
 * User: verber
 * Date: 26.03.13
 * Time: 22:24
 * To change this template use File | Settings | File Templates.
 */

namespace Demos;

use WindowsAzure\Common\ServicesBuilder;
use WindowsAzure\Common\ServiceException;
use Silex\Application;
use Symfony\Component\HttpFoundation\Request;
use WindowsAzure\Queue\Models\CreateQueueOptions;
use WindowsAzure\Queue\Models\ListMessagesOptions;
use WindowsAzure\Queue\Models\ListQueuesOptions;
use WindowsAzure\Queue\Models\PeekMessagesOptions;
use WindowsAzure\Table\Models\EdmType;
use WindowsAzure\Table\Models\Entity;

class Queues {

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

        $this->init();

        $this->tableProxy = ServicesBuilder::getInstance()->createTableService($connectionString);

        $this->initTable();
    }

    private function init()
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

    public function index(Request $request, Application $app)
    {
        $options = new PeekMessagesOptions();
        $options->setNumberOfMessages(5);
        $peekMessageResult = $this->queueProxy->peekMessages('test-queue', $options);

        $view = new View();
        $view['messages'] = $peekMessageResult->getQueueMessages();
        $view['timeout'] = $this->getTimeout();
        $view['count'] = $this->getCount();

        return $view->render('Queues/index.php');

    }

    public function add(Request $request, Application $app)
    {
        $messageText = $request->get('message');
        $this->queueProxy->createMessage('test-queue', $messageText);
        return $app->redirect('/index.php/queues');
    }

    public function manage(Request $request, Application $app)
    {
        $number = (int) $request->get('number')?:1;
        $timeout = (int) $request->get('timeout')?:30;

        $entity = new Entity();
        $entity->setPartitionKey("workerConfig");
        $entity->setRowKey('timeout');
        $entity->addProperty("value", EdmType::INT32, $timeout);
        $this->tableProxy->insertOrReplaceEntity("config", $entity);

        $entity = new Entity();
        $entity->setPartitionKey("workerConfig");
        $entity->setRowKey('count');
        $entity->addProperty("value", EdmType::INT32, $number);
        $this->tableProxy->insertOrReplaceEntity("config", $entity);

        return $app->redirect('/index.php/queues');
    }

}