<?php

declare(strict_types=1);

namespace Rabbit\DB;


use Rabbit\Base\Core\Context;

/**
 * Class DbContext
 * @package Rabbit\DB
 */
class DbContext extends Context
{
    public static function get(string $name, ?string $key = 'database')
    {
        return parent::get($name, $key);
    }

    public static function set(string $name, $value, ?string $key = 'database'): void
    {
        parent::set($name, $value, $key);
    }

    public static function has(string $name, ?string $key = 'database'): bool
    {
        return parent::has($name, $key);
    }

    public static function delete(string $name, ?string $key = 'database'): void
    {
        parent::delete($name, $key);
    }

    public static function release(): void
    {
        $context = getContext();
        if (isset($context['database'])) {
            /** @var ConnectionInterface $connection */
            foreach ($context['database'] as $connection) {
                $connection->release(true);
            }
        }
    }
}
