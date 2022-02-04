<?php

declare(strict_types=1);

namespace Rabbit\DB;

use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\Pool\PdoPool;
use Rabbit\Pool\BaseManager;

class Manager extends BaseManager
{
    public function add(array $configs): void
    {
        foreach ($configs as $name => $config) {
            if (!isset($this->items[$name])) {
                if (!is_array($config)) {
                    $this->items[$name] = $config;
                    continue;
                }
                /** @var PdoPool $pool */
                if ($args = ArrayHelper::remove($config, 'pool')) {
                    $pool = create($args, [], false);
                    $poolConfig = $pool->getPoolConfig();
                    $poolConfig->setUri($config['dsn']);
                    $config['poolKey'] = $poolConfig->getName();
                    $conn = create($config, [], false);
                    $poolConfig->setConfig(['conn' => $conn]);
                } else {
                    $conn = create($config, [], false);
                }
                $this->items[$name] = $conn;
            }
        }
    }
}
