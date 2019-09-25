<?php


namespace SEKafka\Tests\Stubs;


use SEKafka\Kernel\Bus\Dispatchable;
use SEKafka\Kernel\Contract\ShouldQueue;
use SEKafka\Kernel\Support\Queueable;

class ExampleJob implements ShouldQueue
{
    use Dispatchable, Queueable;

    protected $payload;

    public function __construct($payload)
    {
        $this->payload = $payload;
    }

    public function payload()
    {
        file_put_contents('hello.log', 'Hello kafka' . PHP_EOL, FILE_APPEND);
    }

}