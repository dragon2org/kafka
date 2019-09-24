<?php


namespace SEKafka\Queue;


use Exception;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Debug\Exception\FatalThrowableError;
use Throwable;

class WorkCommand extends Command
{
    protected static $defaultName = 'kafka:queue:work';

    protected $exceptions;

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
            'log_level' => LOG_DEBUG,
            'debug' => 'consumer',
            'group.id' => 'CID_alikafka_mini_program_publish_common_test',
            'brokers' => 'docker.for.mac.localhost:9092',
            'queue' => 'alikafka_mini_program_publish_test1_4',
            'offset.store.method' => 'broker',
        ];

        $app = new Application($config);
        $timeout = 3600;
        $queue = $app['kafka.queue'];
        $worker = new Worker($app);


        while(true){
            $job = $worker->getNextJob($queue);

            if ($job) {
                $this->runJob($job);
            } else {
                $this->sleep(2);
            }

            $timeout--;
            if($timeout ==0){
                break;
            }
        }

    }

    /**
     * Sleep the script for a given number of seconds.
     *
     * @param  int|float   $seconds
     * @return void
     */
    public function sleep($seconds)
    {
        if ($seconds < 1) {
            usleep($seconds * 1000000);
        } else {
            sleep($seconds);
        }
    }

    protected function runJob($job)
    {
        try {
            return $this->process($job);
        } catch (Exception $e) {
            $this->exceptions->report($e);

            //TODO::数据库死锁。断开连接
//            $this->stopWorkerIfLostConnection($e);
        } catch (Throwable $e) {
            $this->exceptions->report($e = new FatalThrowableError($e));
            //TODO::数据库死锁。断开连接
//            $this->stopWorkerIfLostConnection($e);
        }
    }

    public function process($job)
    {
        try {
            //TOOD::如果任务已经达到最大次数，标记失败
            $job->fire();

//            $this->raiseAfterJobEvent($connectionName, $job);
        } catch (Exception $e) {
            $this->handleJobException($job, new FatalThrowableError($e));
        } catch (Throwable $e) {
            $this->handleJobException($job, new FatalThrowableError($e));
        }
    }


    protected function handleJobException($job, $e)
    {
        try {
            //TODO::处理最大重试次数标记
        } finally {
            //TODO:: 如果任务最后还是没有正常结束，这里要把任务重新放入队列
        }

        throw $e;
    }
}