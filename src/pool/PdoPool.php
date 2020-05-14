<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 9:36
 */

namespace rabbit\db\pool;

use rabbit\core\ObjectFactory;
use rabbit\pool\ConnectionInterface;
use rabbit\pool\ConnectionPool;

/**
 * Class PdoPool
 * @package rabbit\illuminate\db\pool
 */
class PdoPool extends ConnectionPool
{
    /**
     * @return ConnectionInterface
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
     */
    public function createConnection(): ConnectionInterface
    {
        $poolConfig = $this->getPoolConfig();
        $config = $poolConfig->getConfig();
        $config['poolKey'] = $poolConfig->getName();
        return ObjectFactory::createObject($config, [], false);
    }
}
