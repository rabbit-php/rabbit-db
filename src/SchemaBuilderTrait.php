<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Rabbit\Pool\ConnectionInterface;

trait SchemaBuilderTrait
{
    public function primaryKey(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_PK, $length);
    }

    abstract protected function getDb(): ?ConnectionInterface;

    public function bigPrimaryKey(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_BIGPK, $length);
    }

    public function char(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_CHAR, $length);
    }

    public function string(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_STRING, $length);
    }

    public function text(): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_TEXT);
    }

    public function tinyInteger(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_TINYINT, $length);
    }

    public function smallInteger(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_SMALLINT, $length);
    }

    public function integer(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_INTEGER, $length);
    }

    public function bigInteger(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_BIGINT, $length);
    }

    public function float(int $precision = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_FLOAT, $precision);
    }

    public function double(int $precision = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_DOUBLE, $precision);
    }

    public function decimal(int $precision = null, int $scale = null): ColumnSchemaBuilder
    {
        $length = [];
        if ($precision !== null) {
            $length[] = $precision;
        }
        if ($scale !== null) {
            $length[] = $scale;
        }

        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_DECIMAL, $length);
    }

    public function dateTime(int $precision = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_DATETIME, $precision);
    }

    public function timestamp(int $precision = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_TIMESTAMP, $precision);
    }

    public function time(int $precision = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_TIME, $precision);
    }

    public function date(): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_DATE);
    }

    public function binary(int $length = null): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_BINARY, $length);
    }

    public function boolean(): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_BOOLEAN);
    }

    public function money(int $precision = null, int $scale = null): ColumnSchemaBuilder
    {
        $length = [];
        if ($precision !== null) {
            $length[] = $precision;
        }
        if ($scale !== null) {
            $length[] = $scale;
        }

        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_MONEY, $length);
    }

    public function json(): ColumnSchemaBuilder
    {
        return $this->getDb()->getSchema()->createColumnSchemaBuilder(Schema::TYPE_JSON);
    }
}
