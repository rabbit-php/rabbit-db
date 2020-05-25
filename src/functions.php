<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/30
 * Time: 9:25
 */

if (!function_exists('DBRelease')) {
    function DBRelease(callable $callback)
    {
        $result = call_user_func($callback);
        \rabbit\db\DbContext::release();
        return $result;
    }
}
