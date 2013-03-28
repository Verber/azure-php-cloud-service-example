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
    <?php if (isset($timeout) && $timeout): ?>
        <script type="text/javascript">
            setTimeout(
                function(){
                    document.location.href = '/index.php/queues';
                },
                <?php echo (1000 * $timeout); ?>
            );
        </script>
    <?php endif; ?>
</head>
<body>
<h1>Enqueue a message</h1>
<p>Type message and press Enter or <strong>Submit</strong> to save.</p>
<form method="post" action="/index.php/queues/add" enctype="multipart/form-data" >
    Message  <input type="text" name="message" id="message"/></br>
    <input type="submit" name="submit" value="Submit" />
</form>
<h2>Enqueued messages:</h2>
<?php if (isset($messages) && count($messages)): ?>
<table>
    <tr>
        <th>Id</th>
        <th>Message</th>
        <th>Date</th>
    </tr>
    <?php foreach ($messages as $message): ?>
    <tr>
        <td><?php echo $message->getMessageId(); ?></td>
        <td><?php echo $message->getMessageText(); ?></td>
        <td><?php echo $message->getInsertionDate()->format('r'); ?></td>
    </tr>
    <?php endforeach; ?>
</table>
<?php else: ?>
    <p>There are no messages in queue</p>
<?php endif; ?>
<h2>Dequeue messages</h2>
<form method="post" action="/index.php/queues/list" enctype="multipart/form-data" >
    Dequeue <input type="text" name="number" id="number"/>
    messages on <input type="text" name="timeout" id="timeout"/> seconds
    and
    <select name="action" id="action">
        <option value="Release" selected="selected">Release</option>
        <option value="Delete">Delete</option>
    </select>
    <input type="submit" name="go" value="Go!" />
</form>
<h3>Locked messages list</h3>
<?php if (isset($locked_messages) && count($locked_messages)): ?>
<table>
    <tr>
        <th>Id</th>
        <th>Message</th>
        <th>Date</th>
    </tr>
    <?php foreach ($locked_messages as $locked_message): ?>
    <tr>
        <td><?php echo $locked_message->getMessageId(); ?></td>
        <td><?php echo $locked_message->getMessageText(); ?></td>
        <td><?php echo $locked_message->getInsertionDate()->format('r'); ?></td>
    </tr>
    <?php endforeach; ?>
</table>
<?php else: ?>
    <p>There are no locked messages</p>
<?php endif; ?>
</body>
</html>