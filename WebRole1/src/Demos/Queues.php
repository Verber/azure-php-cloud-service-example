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

class Queues {

    /**
     * @var \WindowsAzure\Queue\QueueRestProxy queueProxy
     */
    private $queueProxy;

    public function __construct()
    {
        $connectionString = 'DefaultEndpointsProtocol=http;'
                    . 'AccountName=phpaz1storage;'
                    . 'AccountKey=nSsL1qPY62PDpeRV2qEAokllKpBRdz8OTkoGt424howtFg/1MdG3slxmsvPwBCOvMcTSu9B/baX6Izy8cikV2A==';
        $this->queueProxy = ServicesBuilder::getInstance()->createQueueService($connectionString);

        $this->init();
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

    public function index(Request $request, Application $app)
    {
        $options = new PeekMessagesOptions();
        $options->setNumberOfMessages(5);
        $peekMessageResult = $this->queueProxy->peekMessages('test-queue', $options);

        $view = new View();
        $view['messages'] = $peekMessageResult->getQueueMessages();

        return $view->render('Queues/index.php');

    }

    public function add(Request $request, Application $app)
    {
        $messageText = $request->get('message');
        $this->queueProxy->createMessage('test-queue', $messageText);
        return $app->redirect('/index.php/queues');
    }

    public function dequeue(Request $request, Application $app)
    {
        $number = $request->get('number')?:1;
        $timeout = $request->get('timeout')?:30;
        $action = $request->get('action')?:'Release';


        $options = new ListMessagesOptions();
        $options->setNumberOfMessages($number);
        $options->setVisibilityTimeoutInSeconds($timeout);

        $listMessagesResult = $this->queueProxy->listMessages('test-queue', $options);
        $locked_messages = $listMessagesResult->getQueueMessages();
        $view = new View();
        $view['locked_messages'] = $listMessagesResult->getQueueMessages();
        $view['timeout'] = $timeout;
        if ('Delete' == $action) {
            foreach ($locked_messages as $message) {
                $messageId = $message->getMessageId();
                $popReceipt = $message->getPopReceipt();
                $this->queueProxy->deleteMessage('test-queue', $messageId, $popReceipt);
            }
        }

        $options = new PeekMessagesOptions();
        $options->setNumberOfMessages(5);
        $peekMessageResult = $this->queueProxy->peekMessages('test-queue', $options);
        $view['messages'] = $peekMessageResult->getQueueMessages();

        return $view->render('Queues/index.php');


    }

}