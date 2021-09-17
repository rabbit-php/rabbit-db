<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use InvalidArgumentException;

class TableSchema
{
    public ?string $schemaName;

    public ?string $name;

    public ?string $fullName;

    public array $primaryKey = [];

    public string $sequenceName;

    public array $foreignKeys = [];

    public array $columns = [];

    public function getColumn(string $name): ?ColumnSchema
    {
        return isset($this->columns[$name]) ? $this->columns[$name] : null;
    }

    public function getColumnNames(): array
    {
        return array_keys($this->columns);
    }

    public function fixPrimaryKey(string|array $keys): void
    {
        $keys = (array)$keys;
        $this->primaryKey = $keys;
        foreach ($this->columns as $column) {
            $column->isPrimaryKey = false;
        }
        foreach ($keys as $key) {
            if (isset($this->columns[$key])) {
                $this->columns[$key]->isPrimaryKey = true;
            } else {
                throw new InvalidArgumentException("Primary key '$key' cannot be found in table '{$this->name}'.");
            }
        }
    }
}
