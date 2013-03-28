<?php
/**
 * Created by JetBrains PhpStorm.
 * User: verber
 * Date: 26.03.13
 * Time: 18:31
 * To change this template use File | Settings | File Templates.
 */

namespace Demos;


class Dashboard {

    public function index()
    {
        $view = new View();
        return $view->render('Dashboard/index.php');
    }
}