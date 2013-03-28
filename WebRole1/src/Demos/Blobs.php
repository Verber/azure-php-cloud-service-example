<?php
namespace Demos;

use Silex\Application;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;
use WindowsAzure\Blob\Models\CreateBlobBlockOptions;
use WindowsAzure\Blob\Models\CreateBlobOptions;
use WindowsAzure\Blob\Models\CreateContainerOptions;
use WindowsAzure\Blob\Models\ListContainersOptions;
use WindowsAzure\Blob\Models\PublicAccessType;
use WindowsAzure\Common\ServicesBuilder;


class Blobs {

    /**
     * @var \WindowsAzure\Blob\Internal\IBlob blobStorage
     */
    private $blobStorage;

    public function __construct()
    {
        $connectionString = 'DefaultEndpointsProtocol=http;'
            . 'AccountName=phpaz1storage;'
            . 'AccountKey=nSsL1qPY62PDpeRV2qEAokllKpBRdz8OTkoGt424howtFg/1MdG3slxmsvPwBCOvMcTSu9B/baX6Izy8cikV2A==';
        $this->blobStorage = ServicesBuilder::getInstance()->createBlobService($connectionString);

        $this->initContainer();

    }

    private function initContainer()
    {
        $listContainersOptions = new ListContainersOptions();
        $listContainersOptions->setPrefix('gallery2');
        $containers = $this->blobStorage->listContainers($listContainersOptions)->getContainers();
        if (count($containers) == 0) {
            $createContainerOptions = new CreateContainerOptions();
            $createContainerOptions->setPublicAccess(PublicAccessType::CONTAINER_AND_BLOBS);
            $this->blobStorage->createContainer('gallery2', $createContainerOptions);
        }
    }

    public function index(Request $request, Application $app)
    {
        $view = new View();
        $view['blobs'] = $this->blobStorage->listBlobs('gallery2')->getBLobs();
        return $view->render('Blobs/index.php');
    }

    public function upload(Request $request, Application $app)
    {
        $image = $request->files->get('image');
        if (is_uploaded_file($image->getPathname())) {
            $blobOptions = new CreateBlobOptions();
            $blobOptions->setContentType($image->getClientMimeType());
            $res = fopen($image->getPathname(), 'r');
            $this->blobStorage->createBlockBlob(
                'gallery2',
                urlencode($image->getClientOriginalName()),
                $res,
                $blobOptions
            );
        }
        return $app->redirect('/index.php/blobs/');
    }

    public function delete(Request $request, Application $app)
    {
        $this->blobStorage->deleteBlob('gallery2', urlencode($request->get('name')));
        return $app->redirect('/index.php/blobs/');
    }
}