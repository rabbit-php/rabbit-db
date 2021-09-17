<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

trait ConstraintFinderTrait
{
    public function getTablePrimaryKey(string $name, bool $refresh = false): ?Constraint
    {
        return $this->getTableMetadata($name, 'primaryKey', $refresh);
    }

    abstract protected function getTableMetadata(string $name, string $type, bool $refresh): null|array|TableSchema;

    public function getSchemaPrimaryKeys(string $schema = '', bool $refresh = false): array
    {
        return $this->getSchemaMetadata($schema, 'primaryKey', $refresh);
    }

    abstract protected function getSchemaMetadata(string $schema, string $type, bool $refresh): array;

    public function getTableForeignKeys(string $name, bool $refresh = false): ?array
    {
        return $this->getTableMetadata($name, 'foreignKeys', $refresh);
    }

    public function getSchemaForeignKeys(string $schema = '', bool $refresh = false): ?array
    {
        return $this->getSchemaMetadata($schema, 'foreignKeys', $refresh);
    }

    public function getTableIndexes(string $name, bool $refresh = false): ?array
    {
        return $this->getTableMetadata($name, 'indexes', $refresh);
    }

    public function getSchemaIndexes(string $schema = '', bool $refresh = false): ?array
    {
        return $this->getSchemaMetadata($schema, 'indexes', $refresh);
    }

    public function getTableUniques(string $name, bool $refresh = false): ?array
    {
        return $this->getTableMetadata($name, 'uniques', $refresh);
    }

    public function getSchemaUniques(string $schema = '', bool $refresh = false): ?array
    {
        return $this->getSchemaMetadata($schema, 'uniques', $refresh);
    }

    public function getTableChecks(string $name, bool $refresh = false): ?array
    {
        return $this->getTableMetadata($name, 'checks', $refresh);
    }

    public function getSchemaChecks(string $schema = '', bool $refresh = false): ?array
    {
        return $this->getSchemaMetadata($schema, 'checks', $refresh);
    }

    public function getTableDefaultValues(string $name, bool $refresh = false): ?array
    {
        return $this->getTableMetadata($name, 'defaultValues', $refresh);
    }

    public function getSchemaDefaultValues(string $schema = '', bool $refresh = false): ?array
    {
        return $this->getSchemaMetadata($schema, 'defaultValues', $refresh);
    }

    abstract protected function loadTablePrimaryKey(string $tableName): ?Constraint;

    abstract protected function loadTableForeignKeys(string $tableName): ?array;

    abstract protected function loadTableIndexes(string $tableName): ?array;

    abstract protected function loadTableUniques(string $tableName): ?array;

    abstract protected function loadTableChecks(string $tableName): ?array;

    abstract protected function loadTableDefaultValues(string $tableName): ?array;
}
