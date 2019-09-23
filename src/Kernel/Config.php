<?php


namespace SEKafka\Kernel;


class Config
{
    protected $config;

    public function __construct(array $config)
    {
        $this->config = $config;
    }

    public function __toArray()
    {
    }

    public function get($key, $default = null)
    {
        if (isset($this->config[$key])) {
            return $this->config[$key];
        }
        return $default;
    }
}