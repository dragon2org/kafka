<?php


namespace SEKafka\Tests\Stubs;


use SEKafka\Kernel\Bus\Dispatchable;
use SEKafka\Kernel\Contract\ShouldQueue;
use SEKafka\Kernel\Support\Queueable;

class ExampleJob implements ShouldQueue
{
    use Dispatchable, Queueable;

    protected $playload;

    public function __construct($playload)
    {
        $this->playload = $playload;
    }

    public function playload()
    {
        print_r($this->playload);
    }

}