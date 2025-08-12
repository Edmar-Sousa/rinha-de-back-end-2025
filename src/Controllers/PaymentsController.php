<?php declare(strict_types=1);

namespace Api\Controllers;

use Swoole\Http\Request as Request;
use Swoole\Http\Response as Response;
use DateTime;

use Redis;

class PaymentsController
{

    public static function createPayment(Request $request, Response $response): void
    {
        $requestBody = json_decode($request->getContent(), true);

        if (!isset($requestBody['amount']) || !isset($requestBody['correlationId'])) {
            $response->status(400);
            $response->end();
            
            return;
        }

        $amount = filter_var(
            $requestBody['amount'],
            FILTER_VALIDATE_FLOAT,
            FILTER_FLAG_ALLOW_FRACTION | FILTER_FLAG_ALLOW_THOUSAND
        );

        if (!$amount) {
            $response->status(400);
            $response->end();

            return;
        }

        $redisClient = new Redis();

        $redisClient->connect($_ENV['REDIS_HOST'], intval($_ENV['REDIS_PORT']));
        $redisClient->auth($_ENV['REDIS_PASSWORD']);

        $redisClient->lPush('payment_queue', json_encode([
            'correlationId' => $requestBody['correlationId'],
            'amount' => $amount,
        ]));

        $redisClient->close();

        $response->status(201);
        $response->end();
    }



    public static function paymentsSummary(Request $request, Response $response, array $query): void
    {
        $startDt = DateTime::createFromFormat('Y-m-d\TH:i:s.v\Z', $query['from']);
        $endDt = DateTime::createFromFormat('Y-m-d\TH:i:s.v\Z', $query['to']);

        $timestampsStart = $startDt->getTimestamp();
        $timestampsEnd = $endDt->getTimestamp();

        $redisClient = new Redis();

        $redisClient->connect($_ENV['REDIS_HOST'], intval($_ENV['REDIS_PORT']));
        $redisClient->auth($_ENV['REDIS_PASSWORD']);

        $payments = $redisClient->zRangeByScore('payments:byRequestedAt', $timestampsStart, $timestampsEnd);

        $responseData = [
            'default' => [
                'totalRequests' => 0,
                'totalAmount' => 0,
            ],
            'fallback' => [
                'totalRequests' => 0,
                'totalAmount' => 0,
            ]
        ];

        foreach ($payments as $paymentId) {
            $payment = $redisClient->hGetAll("payments:{$paymentId}");

            if (!$payment)
                continue;

            $responseData[$payment['processedBy']]['totalRequests'] += 1;
            $responseData[$payment['processedBy']]['totalAmount'] += $payment['amount'];
        }

        $redisClient->close();

        $response->end(json_encode([
            'default' => [
                'totalRequests' => $responseData['default']['totalRequests'],
                'totalAmount' => number_format($responseData['default']['totalAmount'] / 100, 2, '.', ''),
            ],
            'fallback' => [
                'totalRequests' => $responseData['fallback']['totalRequests'],
                'totalAmount' => number_format($responseData['fallback']['totalAmount'] / 100, 2, '.', ''),
            ]
        ]));
    }
}