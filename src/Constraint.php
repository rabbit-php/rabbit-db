<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

/**
 * Constraint represents the metadata of a table constraint.
 *
 * @author Sergey Makinen <sergey@makinen.ru>
 * @since 2.0.13
 */
class Constraint
{
    /**
     * @var string[]|null list of column names the constraint belongs to.
     */
    public ?array $columnNames;
    /**
     * @var string|null the constraint name.
     */
    public ?string $name;
    /**
     * @author Albert <63851587@qq.com>
     * @param [type] $name
     * @param [type] $arguments
     * @return void
     */
    public function __call($name, $arguments)
    {
        if (property_exists($this, $name)) {
            $this->$name = $arguments;
        }
        return $this;
    }
}
