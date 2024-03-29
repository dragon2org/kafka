<?php

/*
 * This file is part of the overtrue/wechat.
 *
 * (c) overtrue <i@overtrue.me>
 *
 * This source file is subject to the MIT license that is bundled
 * with this source code in the file LICENSE.
 */

namespace SEKafak\Kernel\Support;


if (!function_exists('dd')) {
    function dd(...$vars)
    {
        foreach ($vars as $v) {
            print_r($v);
        }

        die(1);
    }
}