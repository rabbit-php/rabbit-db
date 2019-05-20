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
use rabbit\helper\CoroHelper;

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

    /**
     * Manager constructor.
     * @param array $configs
     */
    public function __construct(array $configs)
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
                $pool->getPoolConfig()->setConfig($config);
                $pool->getPoolConfig()->setName($name);
                $this->connections[$name] = $pool;
            }
        }
    }

    /**
     * @param string $name
     * @return Connection
     */
    public function getConnection(string $name = 'db'): Connection
    {
        if (($connection = DbContext::get($name)) === null) {
            /** @var PdoPool $pool */
            $pool = $this->connections[$name];
            $connection = $pool->getConnection();
            DbContext::set($name, $connection);
            if (($cid = CoroHelper::getId()) !== -1 && !array_key_exists($cid, $this->deferList)) {
                defer(function () use ($cid) {
                    DbContext::release();
                    unset($this->deferList[$cid]);
                });
                $this->deferList[$cid] = true;
            }
        }
        return $connection;
    }

    /**
     *
     */
    public function release(): void
    {
        DbContext::release();
    }
}