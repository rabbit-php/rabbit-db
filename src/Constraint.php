<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

class Constraint
{
    public ?array $columnNames;

    public ?string $name;

    public function __call($name, $arguments)
    {
        if (property_exists($this, $name)) {
            $this->$name = $arguments;
        }
        return $this;
    }
}
