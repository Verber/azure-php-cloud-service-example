<?php
include_once('vendor/autoload.php');

$app = new Silex\Application();

/*$app->get('/blobs/', 'Demos\Blobs::index');
$app->post('/blobs/upload', 'Demos\Blobs::upload');
$app->get('/blobs/delete/{name}', 'Demos\Blobs::delete')
    ->assert('name', '.+');

$app->get('/mysql/', 'Demos\MySQL::index');
$app->post('/mysql/register', 'Demos\MySQL::register');

$app->get('/tables/', 'Demos\Tables::index');
$app->post('/tables/save', 'Demos\Tables::save');*/

$app->get('/queues/', 'Demos\Queues::index');
$app->post('/queues/add', 'Demos\Queues::add');
$app->post('/queues/manage', 'Demos\Queues::manage');


$app->get('/info/', function() { phpinfo(); });
$app->get('/', 'Demos\Dashboard::index');

$app['debug'] = true;

$app->run();