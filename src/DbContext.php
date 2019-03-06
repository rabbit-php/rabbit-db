<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 19:52
 */

namespace rabbit\db;


use rabbit\core\Context;
use rabbit\core\ContextTrait;

/**
 * Class DbContext
 * @package rabbit\illuminate\db
 */
class DbContext extends Context
{
    protected static $key = 'database';

    /**
     *
     */
    public static function release(): void
    {
        $context = \Co::getContext();
        if (isset($context[self::$key])) {
            foreach ($context[self::$key] as $name => $connection) {
                $connection->release();
            }
            unset($context[self::$key]);
        }
    }
}