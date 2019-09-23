<?php


namespace SEKafka\Kernel\Providers;


use Pimple\Container;
use Pimple\ServiceProviderInterface;
use RdKafka\Producer;
use RdKafka\TopicConf;

class KafkaTopicConfServiceProvider implements ServiceProviderInterface
{
    public function register(Container $pimple)
    {
        $pimple['topic_conf'] = function ($app) {
            return new TopicConf();
        };
    }
}