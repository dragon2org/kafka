<?php


namespace SEKafka\Queue;


use Pimple\Container;

class Worker
{
    protected $manager;

    protected $container;

    public function __construct(Container $pimple)
    {
        $this->container = $pimple;
    }

    public function getNextJob($connectionName, $queue)
    {

    }

    public function runJob()
    {

    }

    public function sleep()
    {

    }
}