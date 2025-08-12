<?php

require __DIR__ . '/../vendor/autoload.php';

use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\Http\Client;
use Swoole\Coroutine\WaitGroup;

$REDIS_HOST = $_ENV['REDIS_HOST'];
$REDIS_PORT = $_ENV['REDIS_PORT'];
$REDIS_PASSWORD = $_ENV['REDIS_PASSWORD'];
$REDIS_POOL_SIZE = $_ENV['REDIS_POOL_SIZE'];

$PROCESS_COUNT = 3;

$PAYMENT_PROCESSOR_DEFAULT  = $_ENV['API_PAYMENT_DEFAULT'];
$PAYMENT_PROCESSOR_FALLBACK = $_ENV['API_PAYMENT_FALLBACK'];

$PAYMENT_PROCESSOR_PORT = 80;


$apisStatus = [
    'default' => [
        'failing' => false,
        'minResponseTime' => 0,
        'host' => $PAYMENT_PROCESSOR_DEFAULT,
    ],
    'fallback' => [
        'failing' => false,
        'minResponseTime' => 0,
        'host' => $PAYMENT_PROCESSOR_FALLBACK,
    ],
];

Co::set([
    'hook_flags' => SWOOLE_HOOK_ALL
]);


$createRedisConnection = function () use ($REDIS_HOST, $REDIS_PORT, $REDIS_PASSWORD) {
    $redis = new Redis();
    $redis->connect($REDIS_HOST, $REDIS_PORT);
    $redis->auth($REDIS_PASSWORD);

    return $redis;
};

$getServicesAvaible = function () use (&$apisStatus) {
    $fallbackFailing = $apisStatus['fallback']['failing'];
    $defaultFailing = $apisStatus['default']['failing'];

    if ($fallbackFailing && $defaultFailing)
        return null;

    return $defaultFailing ? 'fallback' : 'default';
};


$selectPaymentProcessor = function () use (&$apisStatus, $getServicesAvaible) {
    $serviceAvaible = $getServicesAvaible();

    if ($serviceAvaible == null)
        return null;

    $fallbackResponseTime = $apisStatus['fallback']['minResponseTime'];
    $defaultResponseTime = $apisStatus['default']['minResponseTime'];

    $toleranceRate = 500;
    $bestService = ($defaultResponseTime - $toleranceRate) < $fallbackResponseTime ? 'default' : 'fallback';

    $bestServiceIsFalling = $apisStatus[$bestService]['failing'];

    return $bestServiceIsFalling ? $serviceAvaible : $bestService;
};


$storePaymentProcessed = function ($payment) use (&$createRedisConnection) {
    $redis = $createRedisConnection();

    $uuid = $payment['uuid'];
    $timestamp = $payment['timestamp'];

    $redis->hMSet("payments:{$uuid}", [
        'amount' => $payment['amount'] * 100,
        'requestedAt' => $payment['requestedAt'],
        'processedBy' => $payment['processedBy'],
    ]);

    $redis->zAdd('payments:byRequestedAt', $timestamp, $uuid);
    $redis->close();
};


$retryProcessByAvaibleService = function ($payload) use (&$apisStatus, &$createRedisConnection, $getServicesAvaible, $storePaymentProcessed) {

    $apiAvaible = $getServicesAvaible();

    if ($apiAvaible == null) {
        $redis = $createRedisConnection();

        $redis->lPush('payment_queue', json_encode([
            'correlationId' => $payload['correlationId'],
            'amount' => $payload['amount'],
        ]));
    }

    $url = $apisStatus[$apiAvaible]['host'];

    $client = new Client($url, 8080);
    $client->setHeaders([
        'Accept' => 'application/json',
        'Content-Type' => 'application/json',
    ]);

    $dt = new DateTime("now", new DateTimeZone("UTC"));
    $requestedAt = $dt->format("Y-m-d\TH:i:s.u\Z");


    $requestBody = [
        'correlationId' => $payload['correlationId'],
        'amount' => $payload['amount'],
        'requestedAt' => $requestedAt,
    ];

    $client->post('/payments', json_encode($requestBody));

    if ($client->getStatusCode() >= 200 && $client->getStatusCode() <= 299) {
        $uuid = $payload['correlationId'];
        $timestamp = $dt->getTimestamp();

        $storePaymentProcessed([
            'uuid' => $uuid,
            'timestamp' => $timestamp,

            'amount' => $payload['amount'],
            'requestedAt' => $requestedAt,
            'processedBy' => $apiAvaible,
            'correlationId' => $payload['correlationId'],
        ]);
    }

    else if ($client->getStatusCode() == 500)
        echo "ERROR: " . $apiAvaible . PHP_EOL;

    else 
        echo "ERROR: " . swoole_strerror($client->errCode) . PHP_EOL;
};


$sendHttpRequest = function ($payload) use ($storePaymentProcessed, $retryProcessByAvaibleService, &$selectPaymentProcessor, &$apisStatus) 
{
    $api = $selectPaymentProcessor();

    if ($api == null) {
        echo "APIs NOT FOUND " . PHP_EOL;
        return;
    }

    $url = $apisStatus[$api]['host'];

    $client = new Client($url, 8080);
    $client->setHeaders([
        'Accept' => 'application/json',
        'Content-Type' => 'application/json',
    ]);

    $dt = new DateTime("now", new DateTimeZone("UTC"));
    $requestedAt = $dt->format("Y-m-d\TH:i:s.u\Z");


    $requestBody = [
        'correlationId' => $payload['correlationId'],
        'amount' => $payload['amount'],
        'requestedAt' => $requestedAt,
    ];

    $client->post('/payments', json_encode($requestBody));

    if ($client->getStatusCode() >= 200 && $client->getStatusCode() <= 299) {
        $uuid = $payload['correlationId'];
        $timestamp = $dt->getTimestamp();

        $storePaymentProcessed([
            'uuid' => $uuid,
            'timestamp' => $timestamp,

            'amount' => $payload['amount'],
            'requestedAt' => $requestedAt,
            'processedBy' => $api,
            'correlationId' => $payload['correlationId'],
        ]);
    }

    else if ($client->getStatusCode() == 500)
    {
        $retryProcessByAvaibleService([
            'correlationId' => $payload['correlationId'],
            'amount' => $payload['amount'],
        ]);
    }

    else 
        echo "ERROR: " . swoole_strerror($client->errCode) . PHP_EOL;

    $client->close();
};

$apiCheck = function (&$url) {
    $client = new Client($url, 8080);
    $client->setHeaders([
        'Accept' => 'application/json',
        'Content-Type' => 'application/json',
    ]);

    $client->get('/payments/service-health');
    $responseBody = $client->getBody();
    $client->close();

    return json_decode($responseBody, true);
};

Swoole\Timer::tick(5000, function () use (&$apisStatus, $apiCheck, &$PAYMENT_PROCESSOR_DEFAULT, &$PAYMENT_PROCESSOR_FALLBACK) {

    $results = [];
    $wg = new WaitGroup();

    $wg->add();
    go(function () use (&$results, $apiCheck, &$PAYMENT_PROCESSOR_DEFAULT, &$wg) {
        $results[] = $apiCheck($PAYMENT_PROCESSOR_DEFAULT);
        $wg->done();
    });

    $wg->add();
    go(function () use (&$results, $apiCheck, &$PAYMENT_PROCESSOR_FALLBACK, &$wg) {
        $results[] = $apiCheck($PAYMENT_PROCESSOR_FALLBACK);
        $wg->done();
    });

    $wg->wait();

    echo "HEALTH CHECK: " . json_encode($results) . PHP_EOL;

    $apisStatus['default']['failing'] = $results[0]['failing'];
    $apisStatus['default']['minResponseTime'] = $results[0]['minResponseTime'];

    $apisStatus['fallback']['failing'] = $results[1]['failing'];
    $apisStatus['fallback']['minResponseTime'] = $results[1]['minResponseTime'];
});

Co\run(function () use ($PROCESS_COUNT, $createRedisConnection, $sendHttpRequest) {

    $redisConnectionsPool = new Channel($PROCESS_COUNT);

    for ($count = 0; $count < $PROCESS_COUNT; $count++) 
        $redisConnectionsPool->push($createRedisConnection());

    
    for ($process = 0; $process < $PROCESS_COUNT; $process++) 
    {
        Coroutine::create(function () use ($process, $redisConnectionsPool, $createRedisConnection, $sendHttpRequest) {
            echo "[WORKER $process] started" . PHP_EOL;
            $redisConnection = $redisConnectionsPool->pop();

            while (true) 
            {
                try 
                {
                    $payload = $redisConnection->brPop('payment_queue', 5);

                    if (!isset($payload[1]))
                        continue;

                    $requestBody = json_decode($payload[1], true);
                    $sendHttpRequest($requestBody);
                }

                catch (Exception $err) 
                {
                    echo "[WORKER {$process} error\n";
                    $redisConnection->close();
                    $redisConnectionsPool->push($createRedisConnection());
                }
            }

        });

    }

});
