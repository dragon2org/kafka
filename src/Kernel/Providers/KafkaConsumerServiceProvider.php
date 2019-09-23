<?php


namespace SEKafka\Kernel\Providers;


use Pimple\Container;
use Pimple\ServiceProviderInterface;
use RdKafka\KafkaConsumer;

class KafkaConsumerServiceProvider implements ServiceProviderInterface
{
    public function register(Container $pimple)
    {
        $pimple['consumer'] = function ($app, $conf) {
            return new KafkaConsumer($conf);
        };
    }
}