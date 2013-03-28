<html>
<head>
<Title>Blobs gallery</Title>
<style type="text/css">
    body { background-color: #fff; border-top: solid 10px #000;
        color: #333; font-size: .85em; margin: 20; padding: 20;
        font-family: "Segoe UI", Verdana, Helvetica, Sans-Serif;
    }
    h1, h2, h3,{ color: #000; margin-bottom: 0; padding-bottom: 0; }
    h1 { font-size: 2em; }
    h2 { font-size: 1.75em; }
    h3 { font-size: 1.2em; }
    table { margin-top: 0.75em; }
    th { font-size: 1.2em; text-align: left; border: none; padding-left: 0; }
    td { padding: 0.25em 2em 0.25em 0em; border: 0 none; }
    ul {list-style-image: none;}
    ul li {float: left; display: block; position: relative;}
    ul li a {position: absolute; top: 0; right: 0; color: red; background-color: black;}
</style>
</head>
<body>
<h1>Add image</h1>
<p>Add image and press <strong>Upload</strong> to save.</p>
<form method="post" action="/index.php/blobs/upload" enctype="multipart/form-data" >
      Image <input type="file" name="image" accept="image/*" capture></br>
      <input type="submit" name="upload" value="Upload" />
</form>
<?php if (isset($blobs) && count($blobs)): ?>
<ul>
    <?php foreach ($blobs as $blob): ?>
        <li>
            <img src="<?php echo $blob->getUrl(); ?>"
                 alt="<?php echo $blob->getName(); ?>"
                 title="<?php echo $blob->getName(); ?>" />
            <a href="/index.php/blobs/delete/<?php echo urlencode($blob->getName()); ?>" title="Delete">[x]</a>
        </li>
    <?php endforeach; ?>
</ul>
<?php else: ?>
    <h2>You have no images</h2>
<?php endif; ?>
</body>
</html>