<?php


namespace SEKafka\Queue;


use Pimple\Container;
use Pimple\ServiceProviderInterface;
use RdKafka\KafkaConsumer;
use SEKafka\Queue\KafkaConnector;

class KafkaQueueServiceProvider implements ServiceProviderInterface
{
    public function register(Container $pimple)
    {
        $pimple['kafka.queue'] = function ($app) {
            $connector = new KafkaConnector($app);
            return $connector->connect();
        };
    }
}