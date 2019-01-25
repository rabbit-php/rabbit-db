<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/30
 * Time: 9:25
 */

if (!function_exists('DbRelease')) {
    function DbRelease()
    {
        $db = getDI('db');
        $db->release();
    }
}