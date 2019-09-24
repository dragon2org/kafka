<?php


namespace SEKafka\Tests\Queue;

use SEKafka\Queue\Application;
use SEKafka\Queue\KafkaJob;
use SEKafka\Queue\Worker;
use SEKafka\Tests\Stubs\ExampleJob;
use SEKafka\Tests\TestCase;

class QueueTest extends TestCase
{
    public function testQueuePush()
    {
        $app = new Application($this->getConfig());
        $queue = $app['kafka.queue'];

        $playload = [
            'aa' => 'this'
        ];
        ExampleJob::dispatch($playload, $queue);
    }

    public function testQueuePop()
    {
        $app = new Application($this->getConfig());
        $queue = $app['kafka.queue'];

        $worker = new Worker($app);
        $job = $worker->getNextJob($queue);

    }

    public function testMockJob()
    {
        $message = file_get_contents('message.log');
        $message = unserialize($message);
        $app = new Application($this->getConfig());
        $config = $app['config'];
        $job = new KafkaJob($app, $app['kafka.queue'], $message, $config->get('queue'));
        $job->fire();
    }
}