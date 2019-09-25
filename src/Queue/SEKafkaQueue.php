<?php
/**
 * Created by PhpStorm.
 * User: harlen
 * Date: 2019/9/25
 * Time: 8:19 PM
 */

namespace SEKafka\Queue;


use ErrorException;
use Exception;
use SEKafka\Exceptions\QueueKafkaException;
use SEKafka\Kernel\Config;

class SEKafkaQueue
{

    /**
     * @var string
     */
    protected $defaultQueue;
    /**
     * @var int
     */
    protected $sleepOnError;
    /**
     * @var array
     */
    protected $config;
    /**
     * @var string
     */
    private $correlationId;
    /**
     * @var \RdKafka\Producer
     */
    private $producer;
    /**
     * @var \RdKafka\KafkaConsumer
     */
    private $consumer;
    /**
     * @var array
     */
    private $subscribedQueueNames = [];

    /**
     * @param \RdKafka\Producer $producer
     * @param \RdKafka\KafkaConsumer $consumer
     * @param array $config
     */
    public function __construct(\RdKafka\Producer $producer, \RdKafka\KafkaConsumer $consumer,Config $config)
    {
        $this->defaultQueue = $config->get('queue');
        $this->sleepOnError = $config->get('sleep_on_error');

        $this->producer = $producer;
        $this->consumer = $consumer;
        $this->config = $config;
    }

    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue, []);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        try {
            $topic = $this->getTopic($queue);

            $pushRawCorrelationId = $this->getCorrelationId();

            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $pushRawCorrelationId);
            echo 'pushRawCorrelationId' . $pushRawCorrelationId . PHP_EOL;
            return $pushRawCorrelationId;
        } catch (ErrorException $exception) {
            $this->reportConnectionError('pushRaw', $exception);
        }
    }

    /**
     * Return a Kafka Topic based on the name
     *
     * @param $queue
     *
     * @return \RdKafka\ProducerTopic
     */
    private function getTopic($queue)
    {
        return $this->producer->newTopic($this->getQueueName($queue));
    }


    protected function createPayload($job, $data = '', $queue = null)
    {
        $payload = json_encode($this->createPayloadArray($job, $queue, $data));
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new InvalidPayloadException(
                'Unable to JSON encode payload. Error code: '.json_last_error()
            );
        }

        return $payload;
    }

    protected function createPayloadArray($job, $queue, $data)
    {
        if(is_object($job)){
            return [
                'displayName' => get_class($job),
                'id' => $this->getCorrelationId(),
                'attempts' => 0,
                'retry' => 0,
            ];
        }
    }


    /**
     * Sets the correlation id for a message to be published.
     *
     * @param string $id
     */
    public function setCorrelationId($id)
    {
        $this->correlationId = $id;
    }

    public function getCorrelationId()
    {
        return $this->correlationId ?: uniqid('', true);
    }


    /**
     * @return array
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * @param string $action
     * @param Exception $e
     *
     * @throws QueueKafkaException
     */
    protected function reportConnectionError($action, Exception $e)
    {
        echo 'Kafka error while attempting ' . $action . ': ' . $e->getMessage();
        //Log::error('Kafka error while attempting ' . $action . ': ' . $e->getMessage());

        // If it's set to false, throw an error rather than waiting
        if ($this->sleepOnError === false) {
            throw new QueueKafkaException('Error writing data to the connection with Kafka');
        }

        // Sleep so that we don't flood the log file
        sleep($this->sleepOnError);
    }


    /**
     * @return \RdKafka\KafkaConsumer
     */
    public function getConsumer()
    {
        return $this->consumer;
    }
}