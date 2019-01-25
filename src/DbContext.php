<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 19:52
 */

namespace rabbit\db;


use rabbit\core\ContextTrait;
use rabbit\helper\CoroHelper;

/**
 * Class DbContext
 * @package rabbit\illuminate\db
 */
class DbContext
{
    use ContextTrait;
    /**
     * @var array
     */
    private static $context = [];

    /**
     *
     */
    public static function release(): void
    {
        if (self::$context !== [] && isset(self::$context[CoroHelper::getId()])) {
            foreach (self::$context[CoroHelper::getId()] as $name => $connection) {
                $connection->release();
            }
            unset(self::$context[CoroHelper::getId()]);
        }
    }
}