<html>
<head>
<Title>Todo Items</Title>
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
</style>
</head>
<body>
<h1>Add todo item</h1>
<p>Add job and press <strong>Submit</strong> to save.</p>
<form method="post" action="/index.php/tables/save" enctype="multipart/form-data" >
    Job  <input type="text" name="job" id="job"/></br>
    Due date <input type="text" name="due" id="due"/></br>
    <input type="submit" name="submit" value="Submit" />
</form>
<?php if (isset($entities) && count($entities)): ?>
<h2>Upcoming TODOs:</h2>
<table>
    <tr>
        <th>Job</th>
        <th>Due</th>
    </tr>
    <?php foreach ($entities as $entity): ?>
    <tr>
        <td><?php echo $entity->getPropertyValue('Job'); ?></td>
        <td><?php echo $entity->getPropertyValue('Due')->format('Y-m-d'); ?></td>
    </tr>
    <?php endforeach; ?>
</table>
<?php else: ?>
    <h2>No upcoming TODOs</h2>
<?php endif; ?>
</body>
</html>