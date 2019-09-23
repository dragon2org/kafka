<?php


namespace SEKafka\Queue;

use Pimple\Container;

class KafkaConnector
{
    /**
     * @var Container
     */
    protected $container;

    /**
     * KafkaConnector constructor.
     *
     * @param Container $container
     */
    public function __construct(Container $container)
    {
        $this->container = $container;
    }

    public function connect()
    {
        $config = $this->container['config'];

        $producer = $this->container['producer'];
        $producer->addBrokers($config->get('brokers'));

        $topicConf = $this->container['topic_conf'];
        $topicConf->set('auto.offset.reset', 'largest');
        $conf = $this->container['conf'];
        $conf->set('debug', $config->get('debug'));
        $conf->set('log_level', $config->get('log_level'));
        $conf->set('group.id', $config->get('group.id'));
        $conf->set('metadata.broker.list', $config->get('brokers'));
        $conf->set('enable.auto.commit', 'false');
        $conf->set('offset.store.method', 'broker');
        $conf->setDefaultTopicConf($topicConf);

        $consumer = $this->container->raw('consumer');
        $consumer = call_user_func($consumer, $this->container, $conf);

        return new KafkaQueue(
            $producer,
            $consumer,
            $config
        );
    }
}