<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2019/1/23
 * Time: 14:52
 */

namespace rabbit\db;

use rabbit\core\ObjectFactory;
use rabbit\db\pool\PdoPool;
use rabbit\helper\ArrayHelper;
use rabbit\pool\BaseManager;

/**
 * Class Manager
 * @package rabbit\db
 */
class Manager extends BaseManager
{
    /**
     * @param array $configs
     */
    public function add(array $configs): void
    {
        foreach ($configs as $name => $config) {
            if (!isset($this->connections[$name])) {
                /** @var PdoPool $pool */
                $pool = ArrayHelper::remove($config, 'pool');
                $poolConfig = $pool->getPoolConfig();
                $poolConfig->setUri($config['dsn']);
                $config['poolName'] = $name;
                $config['poolKey'] = $poolConfig->getName();
                $conn = ObjectFactory::createObject($config, [], false);
                $poolConfig->setConfig(['conn' => $conn]);
                $this->connections[$name] = $conn;
            }
        }
    }
}
