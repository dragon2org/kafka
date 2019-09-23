<?php


namespace SEKafka\Kernel\Providers;


use Pimple\Container;
use Pimple\ServiceProviderInterface;
use RdKafka\Producer;

class KafkaProducerServiceProvider implements ServiceProviderInterface
{
    public function register(Container $pimple)
    {
        $pimple['producer'] = function ($app) {
            return new Producer();
        };
    }
}