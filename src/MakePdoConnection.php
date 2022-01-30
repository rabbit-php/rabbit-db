<?php

declare(strict_types=1);

namespace Rabbit\DB;

use Rabbit\DB\Pool\PdoPool;
use Rabbit\Pool\PoolProperties;

class MakePdoConnection
{
    public static function addConnection(
        string $class,
        string $name,
        string $dsn,
        array $pool,
        array $retryHandler = []
    ): void {
        /** @var Manager $manager */
        $manager = service('db');
        if (!$manager->has($name)) {
            $conn = [
                $name => [
                    '{}' => $class,
                    'dsn' => $dsn,
                    'pool' => create([
                        '{}' => PdoPool::class,
                        'poolConfig' => create([
                            '{}' => PoolProperties::class,
                            'minActive' => intval($pool['min'] ?? 3),
                            'maxActive' => intval($pool['max'] ?? 5),
                            'maxWait' => $pool['wait'] ?? 0,
                            'maxRetry' => $pool['retry'] ?? 3
                        ], [], false)
                    ], [], false)
                ]
            ];
            if (!empty($retryHandler)) {
                $conn[$name]['retryHandler'] = create($retryHandler);
            } else {
                $conn[$name]['retryHandler'] = create(RetryHandler::class);
            }
            $manager->add($conn);
        }
    }
}
