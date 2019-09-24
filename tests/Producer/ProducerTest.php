<?php


namespace SEKafka\Tests\Producer;


use RdKafka\Producer;
use RdKafka\Topic;
use SEKafka\Tests\TestCase;

class ProducerTest extends TestCase
{
    public function testProducerMessage()
    {
        $config = $this->getConfig();
        $config['debug'] = 'all';
        $app = new \SEKafka\Queue\Application($config);

        $config = $app['config'];

        $producer = $app['producer'];
        $state = $producer->addBrokers($config->get('brokers'));
        $this->assertGreaterThanOrEqual(1, $state);

        $this->assertInstanceOf(Producer::class, $producer);

        $topic = $producer->newTopic($config->get('queue'));
        $this->assertInstanceOf(Topic::class, $topic);

        $payload = 'this is phpunit stub message';
        $pushRawCorrelationId = uniqid('', true);
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $pushRawCorrelationId);
        $producer->poll(0);
        $this->assertEquals(1, $producer->getOutQLen());
        $producer->poll(50);
    }
}