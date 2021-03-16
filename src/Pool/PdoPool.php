<?php

declare(strict_types=1);

namespace Rabbit\DB\Pool;

use Rabbit\DB\DbContext;
use Rabbit\Pool\ConnectionPool;

/**
 * Class PdoPool
 * @package Rabbit\DB\Pool
 */
class PdoPool extends ConnectionPool
{
    /**
     * @return mixed
     */
    public function create()
    {
        $poolConfig = $this->getPoolConfig();
        $config = $poolConfig->getConfig();
        $conn = $config['conn'];
        $pdo = $conn->createPdoInstance();
        DbContext::set($conn->getPoolKey(), $pdo);
        return $conn;
    }
}
