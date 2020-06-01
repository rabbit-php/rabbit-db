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

/**
 * Class Manager
 * @package rabbit\db
 */
class Manager
{
    /** @var PdoPool[] */
    protected $connections = [];
    /** @var int */
    protected $min = 5;
    /** @var int */
    protected $max = 6;
    /** @var int */
    protected $wait = 0;

    /**
     * Manager constructor.
     * @param array $configs
     */
    public function __construct(array $configs = [])
    {
        $this->addConnection($configs);
    }

    /**
     * @param array $configs
     */
    public function addConnection(array $configs): void
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

    /**
     * @param string $name
     * @return Connection
     */
    public function getConnection(string $name = 'db'): ?Connection
    {
        if (!isset($this->connections[$name])) {
            return null;
        }
        return $this->connections[$name];
    }

    /**
     * @param string $name
     * @return bool
     */
    public function hasConnection(string $name): bool
    {
        return isset($this->connections[$name]);
    }
}
