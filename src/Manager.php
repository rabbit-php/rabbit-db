<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2019/1/23
 * Time: 14:52
 */

namespace rabbit\db;

use rabbit\contract\ReleaseInterface;
use rabbit\core\ObjectFactory;
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
    /** @var array */
    private $yamlList = [];
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
            /** @var PdoPool $pool */
            if (!isset($this->connections[$name])) {
                if (empty($this->yamlList)) {
                    return null;
                }
                $this->createByYaml();
            }
            $pool = $this->connections[$name];
            $connection = $pool->getConnection();
            DbContext::set($name, $connection);
            if (($cid = \Co::getCid()) !== -1 && !array_key_exists($cid, $this->deferList)) {
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

    private function createByYaml(): void
    {
        foreach ($this->yamlList as $fileName) {
            foreach (yaml_parse_file($fileName) as $name => $dbconfig) {
                if (!isset($this->connections[$name])) {
                    if (!isset($dbconfig['class']) || !isset($dbconfig['dsn']) ||
                        !class_exists($dbconfig['class']) || !$dbconfig['class'] instanceof ConnectionInterface) {
                        throw new Exception("The DB class and dsn must be set current class in $fileName");
                    }
                    [$min, $max, $wait] = ArrayHelper::getValueByArray(
                        ArrayHelper::getValue($dbconfig, 'pool', []),
                        ['min', 'max', 'wait'],
                        null,
                        $this->min,
                        $this->max,
                        $this->wait
                    );
                    ArrayHelper::removeKeys($dbconfig, 'class', 'dsn', 'pool');
                    $this->connections[$name] = ObjectFactory::createObject([
                        'class' => $dbconfig['class'],
                        'dsn' => $dbconfig['dsn'],
                        'pool' => ObjectFactory::createObject([
                            'class' => \rabbit\db\pool\PdoPool::class,
                            'poolConfig' => ObjectFactory::createObject([
                                'class' => \rabbit\db\pool\PdoPoolConfig::class,
                                'minActive' => intval($min / swoole_cpu_num()),
                                'maxActive' => intval($max / swoole_cpu_num()),
                                'maxWait' => $wait
                            ], [], false)
                        ], $dbconfig, false)
                    ]);
                }
            }
        }
    }
}
