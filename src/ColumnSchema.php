<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Rabbit\Base\Helper\StringHelper;

class ColumnSchema
{
    public string $name;

    public bool $allowNull;

    public string $type;

    public string $phpType;

    public string $dbType;

    public ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null $defaultValue;

    public array $enumValues;

    public int $size;

    public int $precision;

    public int $scale;

    public bool $isPrimaryKey;

    public bool $autoIncrement = false;

    public bool $unsigned;

    public string $comment;

    public function phpTypecast(ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null $value): ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null
    {
        return $this->typecast($value);
    }

    protected function typecast(ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null $value): ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null
    {
        if (
            $value === ''
            && !in_array(
                $this->type,
                [
                    Schema::TYPE_TEXT,
                    Schema::TYPE_STRING,
                    Schema::TYPE_BINARY,
                    Schema::TYPE_CHAR
                ],
                true
            )
        ) {
            return null;
        }

        if (
            $value === null
            || gettype($value) === $this->phpType
            || $value instanceof ExpressionInterface
            || $value instanceof Query
        ) {
            return $value;
        }

        if (
            is_array($value)
            && count($value) === 2
            && isset($value[1])
            && in_array($value[1], $this->getPdoParamTypes(), true)
        ) {
            return new PdoValue($value[0], $value[1]);
        }

        switch ($this->phpType) {
            case 'resource':
            case 'string':
                if (is_resource($value)) {
                    return $value;
                }
                if (is_float($value)) {
                    // ensure type cast always has . as decimal separator in all locales
                    return StringHelper::floatToString($value);
                }
                if (is_array($value)) {
                    throw new Exception("{$this->name} can not convert to string");
                }
                return (string)$value;
            case 'integer':
                return (int)$value;
            case 'boolean':
                // treating a 0 bit value as false too
                // https://github.com/yiisoft/yii2/issues/9006
                return (bool)$value && $value !== "\0";
            case 'double':
                return (float)$value;
        }

        return $value;
    }

    protected function getPdoParamTypes(): array
    {
        return [
            \PDO::PARAM_BOOL,
            \PDO::PARAM_INT,
            \PDO::PARAM_STR,
            \PDO::PARAM_LOB,
            \PDO::PARAM_NULL,
            \PDO::PARAM_STMT
        ];
    }

    public function dbTypecast(ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null $value): ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null
    {
        return $this->typecast($value);
    }
}
