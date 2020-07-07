<?php
declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

/**
 * ForeignKeyConstraint represents the metadata of a table `FOREIGN KEY` constraint.
 *
 * @author Sergey Makinen <sergey@makinen.ru>
 * @since 2.0.13
 */
class ForeignKeyConstraint extends Constraint
{
    /**
     * @var string|null referenced table schema name.
     */
    public ?string $foreignSchemaName;
    /**
     * @var string referenced table name.
     */
    public string $foreignTableName;
    /**
     * @var string[] list of referenced table column names.
     */
    public array $foreignColumnNames;
    /**
     * @var string|null referential action if rows in a referenced table are to be updated.
     */
    public ?string $onUpdate;
    /**
     * @var string|null referential action if rows in a referenced table are to be deleted.
     */
    public ?string $onDelete;
}
