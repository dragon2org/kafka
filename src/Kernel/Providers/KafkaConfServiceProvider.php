<?php


namespace SEKafka\Kernel\Providers;


use Pimple\Container;
use Pimple\ServiceProviderInterface;
use RdKafka\Conf;

class KafkaConfServiceProvider implements ServiceProviderInterface
{
    public function register(Container $pimple)
    {
        $pimple['conf'] = function ($app) {
            return new Conf();
        };
    }
}