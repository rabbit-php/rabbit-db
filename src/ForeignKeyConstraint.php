<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

class ForeignKeyConstraint extends Constraint
{
    public ?string $foreignSchemaName;

    public string $foreignTableName;

    public array $foreignColumnNames;

    public ?string $onUpdate;

    public ?string $onDelete;
}
