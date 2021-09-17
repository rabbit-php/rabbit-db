<?php
declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

interface ConstraintFinderInterface
{
    public function getTablePrimaryKey(string $name, bool $refresh = false): ?Constraint;

    public function getSchemaPrimaryKeys(string $schema = '', bool $refresh = false): ?array;

    public function getTableForeignKeys(string $name, bool $refresh = false): ?array;

    public function getSchemaForeignKeys(string $schema = '', bool $refresh = false): ?array;

    public function getTableIndexes(string $name, bool $refresh = false): ?array;

    public function getSchemaIndexes(string $schema = '', bool $refresh = false): ?array;

    public function getTableUniques(string $name, bool $refresh = false): ?array;

    public function getSchemaUniques(string $schema = '', bool $refresh = false): ?array;

    public function getTableChecks(string $name, bool $refresh = false): ?array;

    public function getSchemaChecks(string $schema = '', bool $refresh = false): ?array;

    public function getTableDefaultValues(string $name, bool $refresh = false): ?array;

    public function getSchemaDefaultValues(string $schema = '', bool $refresh = false): ?array;
}
