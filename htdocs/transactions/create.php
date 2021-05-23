<?php
include '../functions.php';
$pdo = pdo_connect_mysql();
$msg = '';
// Check if POST data is not empty
if (!empty($_POST)) {
    // Post data not empty insert a new record
    // Set-up the variables that are going to be inserted, we must check if the POST variables exist if not we can default them to blank
    // $id = isset($_POST['id']) && !empty($_POST['id']) && $_POST['id'] != 'auto' ? $_POST['id'] : NULL;
    // Check if POST variable "name" exists, if not default the value to blank, basically the same for all variables
    $date = isset($_POST['date']) ? $_POST['date'] : date('Y-m-d H:i:s');
    $depot = isset($_POST['depot']) ? $_POST['depot'] : '';
    $isin = isset($_POST['isin']) ? $_POST['isin'] : '';
    $count = isset($_POST['count']) ? $_POST['count'] : '';
    $price = isset($_POST['price']) ? $_POST['price'] : '';
	$value = isset($_POST['value']) ? $_POST['value'] : '';
    // Insert new record into the contacts table
    $stmt = $pdo->prepare('INSERT INTO transactions VALUES (NULL, ?, ?, ?, ?, ?, ?)');
    $stmt->execute([$date, $depot, $isin, $count, $price, $value]);
    // Output message
    $msg = 'Created Successfully!';
}
?>

<?=template_header('Create')?>

<div class="content update">
	<h2>Create Transaction</h2>
    <form action="create.php" method="post">
        <label for="id">ID</label>
		<input type="text" name="id" placeholder="26" value="auto" id="id">
		
		<label for="date">Date</label>
		<input type="date" name="date" value="<?=date('Y-m-d\TH:i')?>" id="date">
		
        <label for="depot">Depot</label>
        <input type="text" name="depot" placeholder="Depot 1" id="depot">
		
		<label for="isin">ISIN</label>
        <input type="text" name="isin" placeholder="TEST1234" id="isin">
		
		<label for="count">Count</label>
        <input type="text" name="count" placeholder="11" id="count">
		
		<label for="price">Price</label>
        <input type="text" name="price" placeholder="10,2" id="price">
		
		<label for="value">Value</label>
        <input type="text" name="value" placeholder="1234.5" id="value">
        
        <input type="submit" value="Create">
    </form>
    <?php if ($msg): ?>
    <p><?=$msg?></p>
    <?php endif; ?>
</div>

<?=template_footer()?>
