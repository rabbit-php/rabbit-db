<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 9:36
 */

namespace rabbit\db\pool;

use rabbit\db\DbContext;
use rabbit\pool\ConnectionInterface;
use rabbit\pool\ConnectionPool;

/**
 * Class PdoPool
 * @package rabbit\illuminate\db\pool
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
        DbContext::set($conn->poolName, $pdo, $conn->driver);
        return $conn;
    }
}
