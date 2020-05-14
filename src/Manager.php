<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2019/1/23
 * Time: 14:52
 */

namespace rabbit\db;

use rabbit\contract\ReleaseInterface;
use rabbit\db\pool\PdoPool;
use rabbit\helper\ArrayHelper;

/**
 * Class Manager
 * @package rabbit\db
 */
class Manager implements ReleaseInterface
{
    /** @var PdoPool[] */
    private $connections = [];
    /** @var array */
    private $deferList = [];
    /** @var int */
    private $min = 48;
    /** @var int */
    private $max = 56;
    /** @var int */
    private $wait = 0;

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
                $config['poolName'] = $name;
                $pool->getPoolConfig()->setConfig($config);
                $pool->getPoolConfig()->setUri($config['dsn']);
                $this->connections[$name] = $pool;
            }
        }
    }

    /**
     * @param string $name
     * @return Connection
     */
    public function getConnection(string $name = 'db'): ?Connection
    {
        if (($connection = DbContext::get($name)) === null) {
            $pool = $this->connections[$name];
            $connection = $pool->getConnection();
            DbContext::set($name, $connection);
            if (($cid = \Co::getCid()) !== -1 && !in_array($cid, $this->deferList)) {
                defer(function () use ($cid) {
                    DbContext::release();
                    $this->deferList = array_values(array_diff($this->deferList, [$cid]));
                });
                $this->deferList[] = $cid;
            }
        }
        return $connection;
    }

    /**
     * @param string $name
     * @return bool
     */
    public function hasConnection(string $name): bool
    {
        return isset($this->connections[$name]);
    }

    /**
     *
     */
    public function release(): void
    {
        DbContext::release();
    }
}
