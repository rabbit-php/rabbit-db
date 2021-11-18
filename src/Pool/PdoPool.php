<?php

declare(strict_types=1);

namespace Rabbit\DB\Pool;

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
    public function create(): object
    {
        $poolConfig = $this->getPoolConfig();
        $config = $poolConfig->getConfig();
        $conn = $config['conn'];
        return $conn;
    }
}
