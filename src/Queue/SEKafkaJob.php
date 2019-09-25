<?php

namespace SEKafka\Queue;


use Exception;
use Pimple\Container;
use RdKafka\Message;
use SEKafka\Exceptions\QueueKafkaException;
use SEKafka\Kernel\Support\Str;

class SEKafkaJob
{
    /**
     * @var KafkaQueue
     */
    protected $connection;
    /**
     * @var KafkaQueue
     */
    protected $queue;
    /**
     * @var Message
     */
    protected $message;

    /**
     * KafkaJob constructor.
     *
     * @param Container $container
     * @param KafkaQueue $connection
     * @param Message $message
     * @param $connectionName
     * @param $queue
     */
    public function __construct(Container $container, KafkaQueue $connection, Message $message, $queue)
    {
        $this->connection = $connection;
        $this->message = $message;
        $this->queue = $queue;
    }

    /**
     * Fire the job.
     *
     * @throws Exception
     */
    public function fire()
    {
        try {
            $payload = $this->payload();

            $command = $this->unserialize($payload);

            $command->handle();

            $this->delete();
        } catch (Exception $exception) {
            if (
                $this->causedByDeadlock($exception)
                || Str::contains($exception->getMessage(), ['detected deadlock'])
            ) {
                sleep($this->connection->getConfig()['sleep_on_deadlock']);
                $this->fire();

                return;
            }

            throw $exception;
        }
    }

    /**
     * Determine if the given exception was caused by a deadlock.
     *
     * @param  \Exception  $e
     * @return bool
     */
    protected function causedByDeadlock(Exception $e)
    {
        $message = $e->getMessage();

        return Str::contains($message, [
            'Deadlock found when trying to get lock',
            'deadlock detected',
            'The database file is locked',
            'database is locked',
            'database table is locked',
            'A table in the database is locked',
            'has been chosen as the deadlock victim',
            'Lock wait timeout exceeded; try restarting transaction',
            'WSREP detected deadlock/conflict and aborted the transaction. Try restarting the transaction',
        ]);
    }


    /**
     * Get the decoded body of the job.
     *
     * @return array
     */
    public function payload()
    {
        return json_decode($this->getRawBody(), true);
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @return int
     */
    public function attempts()
    {
        return (int) ($this->payload()['attempts']) + 1;
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->message->payload;
    }

    /**
     * Delete the job from the queue.
     */
    public function delete()
    {
        try {
            $this->connection->getConsumer()->commitAsync($this->message);
        } catch (\RdKafka\Exception $exception) {
            throw new QueueKafkaException('Could not delete job from the queue', 0, $exception);
        }
    }

    /**
     * Unserialize job.
     *
     * @param array $body
     *
     * @throws Exception
     *
     * @return mixed
     */
    private function unserialize(array $body)
    {
        try {
            return unserialize($body['data']['command']);
        } catch (Exception $exception) {
            if (
                $this->causedByDeadlock($exception)
                || Str::contains($exception->getMessage(), ['detected deadlock'])
            ) {
                sleep($this->connection->getConfig()['sleep_on_deadlock']);

                return $this->unserialize($body);
            }

            throw $exception;
        }
    }
}
