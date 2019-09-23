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

$app = new \SEKafka\Queue\Application($config);

$config = $app['config'];

$producer = $app['producer'];
$producer->addBrokers($config->get('brokers'));

$topicConf = $app['topic_conf'];
$topicConf->set('auto.offset.reset', 'largest');
$conf = $app['conf'];
$conf->set('debug', $config->get('debug'));
$conf->set('log_level', $config->get('log_level'));
$conf->set('group.id', $config->get('group.id'));
$conf->set('metadata.broker.list', $config->get('brokers'));
$conf->set('enable.auto.commit', 'false');
$conf->set('offset.store.method', 'broker');
$conf->setDefaultTopicConf($topicConf);

$consumer = $app->raw('consumer');
$consumer = call_user_func($consumer, $app, $conf);
while (true) {
    // The first argument is the partition (again).

    // The second argument is the timeout.
    $msg = $consumer->consume(1000);
    if (null === $msg) {
        continue;
    } elseif ($msg->err) {
        echo $msg->errstr(), "\n";
        break;
    } else {
        echo $msg->payload, "\n";
    }
    die;
}
