<?php


namespace SEKafka\Tests\Consumer;


use SEKafka\Exceptions\QueueKafkaException;
use SEKafka\Kernel\Config;
use SEKafka\Tests\TestCase;

class OriginConsumerTest extends TestCase
{
    public function testConsumerWork()
    {
        $config = $this->getConfig();
        $app = new \SEKafka\Queue\Application($config);

        $config = $app['config'];
        $this->assertInstanceOf(Config::class, $config);

        $producer = $app['producer'];
        $this->assertInstanceOf(\RdKafka\Producer::class, $producer);

        $producer->addBrokers($config->get('brokers'));

        $topicConf = $app['topic_conf'];
        $this->assertInstanceOf(\RdKafka\TopicConf::class, $topicConf);
        $topicConf->set('auto.offset.reset', 'largest');

        $conf = $app['conf'];
        $this->assertInstanceOf(\RdKafka\Conf::class, $conf);

        $conf->set('debug', $config->get('debug'));
        $conf->set('log_level', $config->get('log_level'));
        $conf->set('group.id', $config->get('group.id'));
        $conf->set('metadata.broker.list', $config->get('brokers'));
        $conf->set('enable.auto.commit', 'false');
        $conf->setDefaultTopicConf($topicConf);
        $consumer = $app->raw('consumer');
        $consumer = call_user_func($consumer, $app, $conf);
        $this->assertInstanceOf(\RdKafka\KafkaConsumer::class, $consumer);

        try {
            $consumer->subscribe([$config->get('queue')]);

            $message = $consumer->consume(1000);
            if ($message === null) {
                return null;
            }
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    //TODO::处理消息
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    break;
                default:
                    throw new QueueKafkaException($message->errstr(), $message->err);
            }
        } catch (\RdKafka\Exception $exception) {
            echo 'exception:';
        }
    }
}