<?php

require __DIR__ . '/../vendor/autoload.php';

use Swoole\Http\Server;
use Swoole\Constant;

use Api\Controllers\PaymentsController;

$HOST = '0.0.0.0';
$PORT = 9501;


$httpServer = new Server($HOST, $PORT);

$httpServer->set([
    'worker_num' => 3,
]);

$httpServer->on(Constant::EVENT_START, function () use ($HOST, $PORT) {
    echo "http server is started at http://$HOST:$PORT\n";
});


$routes = [
    '/payments' => [PaymentsController::class, 'createPayment'],
    '/payments-summary' => [PaymentsController::class, 'paymentsSummary'],
];

$httpServer->on(Constant::EVENT_REQUEST, function ($request, $response) use (&$routes) {
    $response->header('Content-Type', 'application/json');
    $uri = $request->server['request_uri'];

    $queryParams = [];

    if (isset($request->server['query_string'])) 
    {
        $queryParamsExploded = explode('&', $request->server['query_string']);
        
        foreach ($queryParamsExploded as $query)
        {
            $param = explode('=', $query);
            $queryParams[$param[0]] = $param[1];
        }
    }

    if (!isset($routes[$uri])) {
        $response->status(404);
        $response->end();

        return;
    }

    call_user_func($routes[$uri], $request, $response, $queryParams);
});


$httpServer->start();
