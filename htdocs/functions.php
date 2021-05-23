<?php


function pdo_connect_mysql() {

	include 'config.php';

    try {
    	return new PDO('mysql:host=' . $DATABASE_HOST . ';dbname=' . $DATABASE_NAME . ';charset=utf8', $DATABASE_USER, $DATABASE_PASS);
    } catch (PDOException $exception) {
    	// If there is an error with the connection, stop the script and display the error.
    	exit('Failed to connect to database!');
    }
}

function template_header($title) {
	
session_start(); /* Starts the session */
if(!isset($_SESSION['UserData']['Username'])){
	header("location:/login.php");
	exit;
}

echo <<<EOT
<!DOCTYPE html>
<html>
	<head>
		<meta charset="utf-8">
		<title>$title</title>
		<link href="/style.css" rel="stylesheet" type="text/css">
		<link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.7.1/css/all.css">
	</head>
	<body>
    <nav class="navtop">
    	<div>
    		<h1>StockSnake</h1>
            <a href="/index.php"><i class="fas fa-home"></i>Home</a>
    		<a href="/transactions/read.php"><i class="fas fa-address-book"></i>Transactions</a>
			<a href="/logout.php"><i class="fas fa-address-book"></i>Log-Out</a>
    	</div>
    </nav>
EOT;
}
function template_footer() {
echo <<<EOT
    </body>
</html>
EOT;
}
?>

