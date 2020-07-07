<?php
declare(strict_types=1);

namespace Rabbit\DB;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\DB\Pool\PdoPool;
use Rabbit\Pool\PoolProperties;
use Throwable;

/**
 * Class MakePdoConnection
 * @package rabbit\db\mysql
 */
class MakePdoConnection
{
    /**
     * @param string $class
     * @param string $name
     * @param string $dsn
     * @param array $pool
     * @param array $retryHandler
     * @param array $config
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Throwable
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
                    'pool' => create([
                        'class' => PdoPool::class,
                        'poolConfig' => create([
                            'class' => PoolProperties::class,
                            'minActive' => intval($pool['min']),
                            'maxActive' => intval($pool['max']),
                            'maxWait' => $pool['wait'],
                            'maxRetry' => $pool['retry']
                        ], [], false)
                    ], [], false)
                ]
            ];
            if (!empty($retryHandler)) {
                $conn[$name]['retryHandler'] = create($retryHandler);
            } else {
                $conn[$name]['retryHandler'] = getDI(RetryHandlerInterface::class);
            }
            $manager->add($conn);
        }
    }
}
