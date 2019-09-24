<?php


namespace SEKafka\Queue;


use Pimple\Container;
use Symfony\Component\VarDumper\VarDumper;

class Worker
{
    protected $container;

    public function __construct(Container $pimple)
    {
        $this->container = $pimple;
    }

    public function getNextJob($connection)
    {
        $job = $connection->pop();
    }

    public function runJob()
    {

    }

    public function sleep()
    {

    }
}