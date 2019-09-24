<?php

/*
 * This file is part of the overtrue/wechat.
 *
 * (c) overtrue <i@overtrue.me>
 *
 * This source file is subject to the MIT license that is bundled
 * with this source code in the file LICENSE.
 */

namespace SEKafka\Tests;

use PHPUnit\Framework\TestCase as BaseTestCase;

/**
 * class TestCase.
 */
class TestCase extends BaseTestCase
{
    public function getConfig()
    {
        $config = [
//            'KAFKA_BROKERS' => '172.19.0.1:9092',
//            'KAFKA_QUEUE' => 'alikafka_mini_program_publish_test1_4',
//            'KAFKA_CONSUMER_ID' => 'CID_alikafka_mini_program_publish_common_test',
//            'KAFKA_SASL_ENABLE' => false,
//            'KAFKA_SASL_PLAIN_USERNAME' => 'LTAIvmHWk2C9YURr',
//            'KAFKA_SASL_PLAIN_PASSWORD' => '24tzhnjU3',
            'log_level' => LOG_DEBUG,
            'debug' => 'consumer',
            'group.id' => 'CID_alikafka_mini_program_publish_common_test',
            'brokers' => 'docker.for.mac.localhost:9092',
            'queue' => 'alikafka_mini_program_publish_test1_4',
            'offset.store.method' => 'broker',
        ];

        return $config;
    }

    /**
     * Tear down the test case.
     */
    public function tearDown()
    {
        $this->finish();
        parent::tearDown();
    }

    /**
     * Run extra tear down code.
     */
    protected function finish()
    {
        // call more tear down methods
    }
}
