<?php
/**
 * Created by JetBrains PhpStorm.
 * User: verber
 * Date: 26.03.13
 * Time: 17:20
 * To change this template use File | Settings | File Templates.
 */

namespace Demos;

use WindowsAzure\Common\ServicesBuilder;
use WindowsAzure\Common\ServiceException;
use Silex\Application;
use Symfony\Component\HttpFoundation\Request;
use WindowsAzure\Table\Models\EdmType;
use WindowsAzure\Table\Models\Entity;

class Tables {

    /**
     * @var \WindowsAzure\Table\TableRestProxy tableProxy
     */
    private $tableProxy;

    public function __construct()
    {
        $connectionString = 'DefaultEndpointsProtocol=http;'
            . 'AccountName=phpaz1storage;'
            . 'AccountKey=nSsL1qPY62PDpeRV2qEAokllKpBRdz8OTkoGt424howtFg/1MdG3slxmsvPwBCOvMcTSu9B/baX6Izy8cikV2A==';
        $this->tableProxy = ServicesBuilder::getInstance()->createTableService($connectionString);

        $this->initTable();
    }

    private function initTable()
    {
        $existingTables = $this->tableProxy->queryTables('todos');
        if (count($existingTables) == 0) {
            $this->tableProxy->createTable("todos");
        }
    }

    public function index(Request $request, Application $app)
    {
        $view = new View();
        // Retrieve data
        $currentDate = new \DateTime();
        //for datetime we need set type explicitly
        $filter = "Due ge datetime'" . $currentDate->format('Y-m-d\TH:i:s\Z') . "'";
        $view['entities'] = $this->tableProxy->queryEntities("todos", $filter)->getEntities();
        return $view->render('Tables/index.php');
    }

    public function save(Request $request, Application $app)
    {
        $job = $request->get('job');
        $dueDate = new \DateTime($request->get('due'));

        $entity = new Entity();
        $entity->setPartitionKey("todoAzure");
        $entity->setRowKey(uniqid('job'));
        $entity->addProperty("Job", EdmType::STRING, $job);
        $entity->addProperty("Due", EdmType::DATETIME, $dueDate);
        $this->tableProxy->insertEntity("todos", $entity);

        return $app->redirect('/index.php/tables/');
    }

}