<?php
include '../functions.php';
$pdo = pdo_connect_mysql();
$msg = '';
// Check if the contact id exists, for example update.php?id=1 will get the contact with the id of 1
if (isset($_GET['id'])) {
    if (!empty($_POST)) {
        // This part is similar to the create.php, but instead we update a record and not insert
    $date = isset($_POST['date']) ? $_POST['date'] : date('Y-m-d H:i:s');
    $depot = isset($_POST['depot']) ? $_POST['depot'] : '';
    $isin = isset($_POST['isin']) ? $_POST['isin'] : '';
    $count = isset($_POST['count']) ? $_POST['count'] : '';
    $price = isset($_POST['price']) ? $_POST['price'] : '';
	$value = isset($_POST['value']) ? $_POST['value'] : '';
        // Update the record
        $stmt = $pdo->prepare('UPDATE transactions SET date = ?, depot = ?, isin = ?, count = ?, price = ?, value = ? WHERE id = ?');
        $stmt->execute([$date, $depot, $isin, $count, $price, $value, $_GET['id']]);
        $msg = 'Updated Successfully!';
    }
    // Get the contact from the contacts table
    $stmt = $pdo->prepare('SELECT * FROM transactions WHERE id = ?');
    $stmt->execute([$_GET['id']]);
    $record = $stmt->fetch(PDO::FETCH_ASSOC);
    if (!$record) {
        exit('Transaction doesn\'t exist with that ID!');
    }
} else {
    exit('No ID specified!');
}
?>

<?=template_header('Read')?>

<div class="content update">
	<h2>Update Transaction #<?=$record['id']?></h2>
    <form action="update.php?id=<?=$record['id']?>" method="post">
        <label for="id">ID</label>
		<input type="text" name="id" placeholder="26" value="auto" id="id">
		
		<label for="date">Date</label>
		<input type="date" name="date" value="<?=$record['date']?>" id="date">
		
        <label for="depot">Depot</label>
        <input type="text" name="depot" placeholder="Depot 1" value="<?=$record['depot']?>" id="depot">
		
		<label for="isin">ISIN</label>
        <input type="text" name="isin" placeholder="TEST1234" value="<?=$record['isin']?>" id="isin">
		
		<label for="count">Count</label>
        <input type="text" name="count" placeholder="11" value="<?=$record['count']?>" id="count">
		
		<label for="price">Price</label>
        <input type="text" name="price" placeholder="10,2" value="<?=$record['price']?>" id="price">
		
		<label for="value">Value</label>
        <input type="text" name="value" placeholder="1234.5" value="<?=$record['value']?>" id="value">
		
        <input type="submit" value="Update">
    </form>
    <?php if ($msg): ?>
    <p><?=$msg?></p>
    <?php endif; ?>
</div>

<?=template_footer()?>