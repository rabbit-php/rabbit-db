<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 9:49
 */

namespace rabbit\db\pool;


use rabbit\pool\PoolProperties;

/**
 * Class PdoPoolConfig
 * @package rabbit\illuminate\db\pool
 */
class PdoPoolConfig extends PoolProperties
{
    /** @var array */
    private $config = [];

    /**
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
    }

    /**
     * @param array $config
     */
    public function setConfig(array $config)
    {
        $this->config = $config;
    }

    /**
     * @param string $name
     */
    public function setName(string $name)
    {
        $this->name = $name;
    }
}