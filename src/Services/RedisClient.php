<?php declare(strict_types=1);

namespace Api\Services;


class RedisClient
{

    public Redis|null $redisClient = null;


    private function getConnection()
    {
        $this->redisClient = new Redis();

        $this->redisClient->connect($_ENV['REDIS_HOST'], $_ENV['REDIS_PORT']);
        $this->redisClient->auth($_ENV['REDIS_PASSWORD']);

        return $this->redisClient;
    }


    public function enqueuePaymentIntention(array $payment): void
    {
        $client = $this->getConnection();
        $client->lPush("payment_queue", json_encode($payment));
    }


    public function getPaymentsFromRange($from, $to) 
    {
        $client = $this->getConnection();
        return $client->zRangeByScore('payments:byRequestedAt', $from, $to);
    }


    public function getPayments($uuid) 
    {
        $client = $this->getConnection();
        return $client->hGetAll("payments:{$uuid}");
    }
    
}