<?php
/**
 * Created by JetBrains PhpStorm.
 * User: verber
 * Date: 26.03.13
 * Time: 16:52
 * To change this template use File | Settings | File Templates.
 */

namespace Demos;

use Silex\Application;
use Symfony\Component\HttpFoundation\Request;

class MySQL {

    /**
     * @var \PDO connection;
     */
    private $connection;

    public function __construct()
    {
        $host = "eu-cdbr-azure-north-a.cloudapp.net";
        $user = "b5a0d39aaf6422";
        $pwd = "a9f91b2a";
        $db = "phpaz1MySQL";
        $this->connection = new \PDO( "mysql:host=$host;dbname=$db", $user, $pwd);
        $this->connection->setAttribute( \PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION );
        $this->init();
    }

    private function init()
    {
        $create_sql = "CREATE TABLE IF NOT EXISTS registration_tbl(
        id INT NOT NULL AUTO_INCREMENT,
        PRIMARY KEY(id),
        name VARCHAR(30),
        email VARCHAR(30),
        date DATE)";
        $this->connection->exec($create_sql);
    }

    public function index(Request $request, Application $app)
    {
        $view = new View();
        $sql_select = "SELECT * FROM registration_tbl";
        $stmt = $this->connection->query($sql_select);
        $view['registrants'] = $stmt->fetchAll();
        return $view->render('MySQL/index.php');
    }

    public function register(Request $request, Application $app)
    {
        $name = $request->get('name');
        $email = $request->get('email');
        $date = date("Y-m-d");
        // Insert data
        $sql_insert = "INSERT INTO registration_tbl (name, email, date)
                   VALUES (?,?,?)";
        $stmt = $this->connection->prepare($sql_insert);
        $stmt->bindValue(1, $name);
        $stmt->bindValue(2, $email);
        $stmt->bindValue(3, $date);
        $stmt->execute();

        return $app->redirect('/index.php/mysql/');
    }

}