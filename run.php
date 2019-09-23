<?php
use Pimple\Container;
use SEKafka\Kernel\ServiceContainer;

require_once "vendor/autoload.php";

$config = [
    'KAFKA_BROKERS' => 'docker.for.mac.localhost:9092',
    'KAFKA_QUEUE' => 'alikafka_mini_program_publish_test1_4',
    'KAFKA_CONSUMER_ID' => 'CID_alikafka_mini_program_publish_common_test',
    'KAFKA_SASL_ENABLE' => false,
    'KAFKA_SASL_PLAIN_USERNAME' => 'LTAIvmHWk2C9YURr',
    'KAFKA_SASL_PLAIN_PASSWORD' => '24tzhnjU3',

    'log_level' => LOG_DEBUG,
    'debug' => 'consumer',
    'group.id' => 'CID_alikafka_mini_program_publish_common_test',
    'brokers' => 'docker.for.mac.localhost:9092',
    'queue' => 'alikafka_mini_program_publish_test1_4',
];

$container = new ServiceContainer($config);

$conc = new \SEKafka\Queue\KafkaConnector($container);
$queue = $conc->connect();
class a{
    public $bb = '123';
}
$queue->push(['class'=> 123123], ['xxx' => 123]);
echo 'ok';die;

$container['config'] = [
    'KAFKA_BROKERS' => 'docker.for.mac.localhost:9092',
    'KAFKA_QUEUE' => 'alikafka_mini_program_publish_test1_4',
    'KAFKA_CONSUMER_ID' => 'CID_alikafka_mini_program_publish_common_test',
    'KAFKA_SASL_ENABLE' => false,
    'KAFKA_SASL_PLAIN_USERNAME' => 'LTAIvmHWk2C9YURr',
    'KAFKA_SASL_PLAIN_PASSWORD' => '24tzhnjU3',
    'log_level' => LOG_DEBUG,
    'debug' => 'consumer',
];
$container['queue.kafka.conf'] = function ($app){
    $config = new \RdKafka\Conf();
    $config->set('debug', $app['config']['debug']);
    $config->set('log_level', $app['config']['log_level']);
    $config->set('group.id', $app['config']['KAFKA_CONSUMER_ID']);
    $config->set('metadata.broker.list', $app['config']['KAFKA_BROKERS']);
    return $config;
};

$container['queue.kafka.producer'] = function ($app){
    $rk = new \RdKafka\Producer($app['queue.kafka.conf']);
    $rk->addBrokers($app['config']['KAFKA_BROKERS']);
    return $rk;
};

//$producer = $container['queue.kafka.producer'];
//$topic  = $producer->newTopic($container['config']['KAFKA_QUEUE']);
//$topic->produce(RD_KAFKA_PARTITION_UA, 0, "Message payload");
//$producer->flush(1);


$container['queue.kafka.consumer'] = function ($app){
    $rk = new \RdKafka\Consumer($app['queue.kafka.conf']);
    $rk->addBrokers($app['config']['KAFKA_BROKERS']);
    return $rk;
};

$topic = $container['queue.kafka.consumer']->newTopic($container['config']['KAFKA_QUEUE']);
$topic->consumeStart(0, RD_KAFKA_OFFSET_BEGINNING);
while(true){
    // The first argument is the partition (again).
    // The second argument is the timeout.
    $msg = $topic->consume(0, 1000);
    if (null === $msg) {
        continue;
    } elseif ($msg->err) {
        echo $msg->errstr(), "\n";
        break;
    } else {
        echo $msg->payload, "\n";
    }
}

echo 'end';die;

$container['queue.kafka.topic_conf'] = function($app){
    return new \RdKafka\TopicConf();
};



$connect = new \SEKafka\Queue\KafkaConnector\KafkaConnector($container);
$connect->connect([]);

