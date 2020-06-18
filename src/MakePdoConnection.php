<?php


namespace rabbit\db;

use rabbit\core\ObjectFactory;
use rabbit\db\mysql\RetryHandler;

/**
 * Class MakePdoConnection
 * @package rabbit\db\mysql
 */
class MakePdoConnection
{
    /**
     * @param string $name
     * @param string $dsn
     * @param array $pool
     * @param array $config
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public static function addConnection(
        string $class,
        string $name,
        string $dsn,
        array $pool,
        array $retryHandler = [],
        array $config = null
    ): void
    {
        /** @var Manager $manager */
        $manager = getDI('db');
        if (!$manager->has($name)) {
            $conn = [
                $name => [
                    'class' => $class,
                    'dsn' => $dsn,
                    'pool' => ObjectFactory::createObject([
                        'class' => \rabbit\db\pool\PdoPool::class,
                        'poolConfig' => ObjectFactory::createObject([
                            'class' => \rabbit\db\pool\PdoPoolConfig::class,
                            'minActive' => intval($pool['min'] ),
                            'maxActive' => intval( $pool['max']),
                            'maxWait' => $pool['wait'],
                            'maxReconnect' => $pool['retry']
                        ], [], false)
                    ], [], false)
                ]
            ];
            if (!empty($retryHandler)) {
                $conn[$name]['retryHandler'] = ObjectFactory::createObject($retryHandler);
            } else {
                $conn[$name]['retryHandler'] = getDI(RetryHandler::class);
            }
            $manager->add($conn);
        }
    }
}
