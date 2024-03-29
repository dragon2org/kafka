<?php


namespace SEKafka\Queue;

use ErrorException;
use Exception;
use SEKafka\Exceptions\QueueKafkaException;
use SEKafka\Kernel\Support\Queue;
use Symfony\Component\VarDumper\VarDumper;

class KafkaQueue extends Queue
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
    public function __construct($container, \RdKafka\Producer $producer, \RdKafka\KafkaConsumer $consumer, $config)
    {
        $this->container = $container;

        $this->defaultQueue = $config->get('queue');
        $this->sleepOnError = $config->get('sleep_on_error');

        $this->producer = $producer;
        $this->consumer = $consumer;
        $this->config = $config;
    }

    /**
     * Push a new job onto the queue.
     *
     * @param string $job
     * @param mixed $data
     * @param string $queue
     *
     * @return bool
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue, []);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param string $payload
     * @param string $queue
     * @param array $options
     *
     * @throws QueueKafkaException
     *
     * @return mixed
     */
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
     * Push a new job onto the queue after a delay.
     *
     * @param \DateTime|int $delay
     * @param string $job
     * @param mixed $data
     * @param string $queue
     *
     * @throws QueueKafkaException
     *
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        //Later is not sup
        throw new QueueKafkaException('Later not yet implemented');
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string|null $queue
     *
     * @throws QueueKafkaException
     *
     * @return \Illuminate\Queue\Jobs\Job|null
     */
    public function pop($queue = null)
    {
        try {
            $queue = $this->getQueueName($queue);
            if (!in_array($queue, $this->subscribedQueueNames)) {
                $this->subscribedQueueNames[] = $queue;
                $this->consumer->subscribe($this->subscribedQueueNames);
            }
            $message = $this->consumer->consume(10000);

            if ($message === null) {
                return null;
            }
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    return new KafkaJob(
                        $this->container, $this, $message,
                        $this->connectionName, $queue ?: $this->defaultQueue
                    );
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

    /**
     * @param string $queue
     *
     * @return string
     */
    private function getQueueName($queue)
    {
        return $queue ?: $this->defaultQueue;
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

    /**
     * Sets the correlation id for a message to be published.
     *
     * @param string $id
     */
    public function setCorrelationId($id)
    {
        $this->correlationId = $id;
    }

    /**
     * Retrieves the correlation id, or a unique id.
     *
     * @return string
     */
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
     * Create a payload array from the given job and data.
     *
     * @param  string $job
     * @param  mixed $data
     * @param  string $queue
     *
     * @return array
     */
    protected function createPayloadArray($job, $data = '', $queue = null)
    {
        return array_merge(parent::createPayloadArray($job, $data), [
            'id' => $this->getCorrelationId(),
            'attempts' => 0,
        ]);
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