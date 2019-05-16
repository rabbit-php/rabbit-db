<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 19:52
 */

namespace rabbit\db;


use rabbit\core\Context;

/**
 * Class DbContext
 * @method void release()
 * @package rabbit\db
 */
class DbContext extends Context
{
    /** @var self */
    private static $instance;

    /**
     * @param $name
     * @param $arguments
     */
    public static function __callStatic($name, $arguments)
    {
        $name .= 'Context';
        if (!self::$instance) {
            self::$instance = new static();
        }
        return self::$instance->$name(...$arguments);
    }

    /**
     *
     */
    public function releaseContext(): void
    {
        foreach ($this->context as $name => $connection) {
            $connection->release();
            unset($this->context[$name]);
        }
    }
}