<?php


namespace rabbit\db;

use rabbit\core\ObjectFactory;

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
        array $config = null
    ): void {
        /** @var Manager $manager */
        $manager = getDI('db');
        if (!$manager->hasConnection($name)) {
            $manager->addConnection([
                $name => [
                    'class' => $class,
                    'dsn' => $dsn,
                    'pool' => ObjectFactory::createObject([
                        'class' => \rabbit\db\pool\PdoPool::class,
                        'poolConfig' => ObjectFactory::createObject([
                            'class' => \rabbit\db\pool\PdoPoolConfig::class,
                            'minActive' => intval($pool['min'] / swoole_cpu_num()),
                            'maxActive' => intval($pool['max'] / swoole_cpu_num()),
                            'maxWait' => $pool['wait']
                        ], [], false)
                    ], [], false)
                ]
            ]);
        }
    }
}