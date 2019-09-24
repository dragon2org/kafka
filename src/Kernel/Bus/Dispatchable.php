<?php

namespace SEKafka\Kernel\Bus;


trait Dispatchable
{
    /**
     * Dispatch the job with the given arguments.
     *
     * @return \SEKafka\Kernel\Bus\PendingDispatch
     */
    public static function dispatch()
    {
        $queue = func_get_args()[1];
        $object = new static(...func_get_args());

        $queue->push($object);

        echo 'push success';
    }
}