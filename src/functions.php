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
        /** @var \rabbit\db\Manager $db */
        $db = getDI('db');
        $db->release();
    }
}
