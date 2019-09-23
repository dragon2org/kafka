<?php


namespace SEKafka\Queue;


use SEKafka\Kernel\ServiceContainer;

class Application extends ServiceContainer
{
    protected $providers = [
        KafkaQueueServiceProvider::class
    ];
}