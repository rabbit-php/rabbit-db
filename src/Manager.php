<?php
declare(strict_types=1);

namespace Rabbit\DB;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\Pool\PdoPool;
use Rabbit\Pool\BaseManager;

/**
 * Class Manager
 * @package rabbit\db
 */
class Manager extends BaseManager
{
    /**
     * @param array $configs
     * @throws DependencyException
     * @throws NotFoundException
     */
    public function add(array $configs): void
    {
        foreach ($configs as $name => $config) {
            if (!isset($this->items[$name])) {
                /** @var PdoPool $pool */
                $pool = ArrayHelper::remove($config, 'pool');
                $poolConfig = $pool->getPoolConfig();
                $poolConfig->setUri($config['dsn']);
                $config['poolName'] = $name;
                $config['poolKey'] = $poolConfig->getName();
                $conn = create($config, [], false);
                $poolConfig->setConfig(['conn' => $conn]);
                $this->items[$name] = $conn;
            }
        }
    }
}
