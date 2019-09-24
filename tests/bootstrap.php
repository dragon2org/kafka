<?php

/*
 * This file is part of the overtrue/wechat.
 *
 * (c) overtrue <i@overtrue.me>
 *
 * This source file is subject to the MIT license that is bundled
 * with this source code in the file LICENSE.
 */

use Symfony\Component\VarDumper\VarDumper;

define('TEST_ROOT', __DIR__);

$_SERVER['HTTP_HOST'] = 'localhost';

include __DIR__.'/../vendor/autoload.php';


if (!function_exists('dd')) {
    function dd(...$vars)
    {
        foreach ($vars as $v) {
            VarDumper::dump($v);
        }

        die(1);
    }
}
