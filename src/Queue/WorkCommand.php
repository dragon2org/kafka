<?php


namespace SEKafka\Queue;


use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;

class WorkCommand extends Command
{
    protected static $defaultName = 'kafka:queue:work';

    protected function configure()
    {
        $this->setDescription('kafka队列任务处理');
    }

    /**
     * The queue worker instance.
     *
     * @var \RdKafka\Queue\Worker
     */
    protected $worker;

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $config = [
            'KAFKA_BROKERS' => 'docker.for.mac.localhost:9092',
            'KAFKA_QUEUE' => 'alikafka_mini_program_publish_test1_4',
            'KAFKA_CONSUMER_ID' => 'CID_alikafka_mini_program_publish_common_test',
            'KAFKA_SASL_ENABLE' => false,
            'KAFKA_SASL_PLAIN_USERNAME' => 'LTAIvmHWk2C9YURr',
            'KAFKA_SASL_PLAIN_PASSWORD' => '24tzhnjU3',

            'log_level' => LOG_DEBUG,
            'debug' => 'all',
            'group.id' => 'CID_alikafka_mini_program_publish_common_test',
            'brokers' => 'docker.for.mac.localhost:9092',
            'queue' => 'alikafka_mini_program_publish_test1_4',
        ];
        $app = new Application($config);
        echo 'kafka: result:' . PHP_EOL;
        $result = $app['kafka.queue']->pop();
        print_r($result);
        echo 'kafka: result end:' . PHP_EOL;die;

    }
}