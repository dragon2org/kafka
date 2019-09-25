<?php


namespace SEKafka\Tests\Stubs;


class Hello
{
    public function hello()
    {
        file_put_contents('hello.log', 'Hello kafka' . PHP_EOL, FILE_APPEND);
    }
}