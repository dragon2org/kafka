<?php


namespace SEKafka\Tests\Queue;


use SEKafka\Queue\Application;
use SEKafka\Tests\Stubs\Hello;
use SEKafka\Tests\TestCase;

class SEKafkaTest extends TestCase
{
    public function testSimpleQueuePush()
    {
        $app = new Application($this->getConfig());
        $queue = $app['kafka.queue'];
        $a = new Hello();
        $payload = [
            'class' => $a,
            'method' => 't',
            'params' => ['Hello kafka'],
        ];

        $queue->push($payload);
    }

    public function testSimpleQueueJob()
    {
        $app = new Application($this->getConfig());
        $queue = $app['kafka.queue'];
        $consumer = $queue->getConsumer();
        $consumer->subscribe(['alikafka_mini_program_publish_test1_4']);
        try {
            $message = $consumer->consume(100000);

            if ($message === null) {
                return null;
            }
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $payload = json_decode($message->payload, true);
                    $command = unserialize($payload['data']);
                    call_user_func([$command['class'], 'hello'], $command['params']);

//                    return new KafkaJob(
//                        $this->container, $this, $message,
//                        $this->connectionName, $queue ?: $this->defaultQueue
//                    );
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new QueueKafkaException($message->errstr(), $message->err);
            }
        } catch (\RdKafka\Exception $exception) {
            throw new QueueKafkaException('Could not pop from the queue', 0, $exception);
        }
    }

    public function testSimpleQueuePop()
    {
        $app = new Application($this->getConfig());
        $queue = $app['kafka.queue'];
    }

    public function t($params)
    {
        file_put_contents('con.log', $params . PHP_EOL, FILE_APPEND);
    }
}