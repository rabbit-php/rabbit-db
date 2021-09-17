<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Rabbit\Base\Helper\StringHelper;

class ColumnSchemaBuilder
{
    const CATEGORY_PK = 'pk';
    const CATEGORY_STRING = 'string';
    const CATEGORY_NUMERIC = 'numeric';
    const CATEGORY_TIME = 'time';
    const CATEGORY_OTHER = 'other';

    public array $categoryMap = [
        Schema::TYPE_PK => self::CATEGORY_PK,
        Schema::TYPE_UPK => self::CATEGORY_PK,
        Schema::TYPE_BIGPK => self::CATEGORY_PK,
        Schema::TYPE_UBIGPK => self::CATEGORY_PK,
        Schema::TYPE_CHAR => self::CATEGORY_STRING,
        Schema::TYPE_STRING => self::CATEGORY_STRING,
        Schema::TYPE_TEXT => self::CATEGORY_STRING,
        Schema::TYPE_TINYINT => self::CATEGORY_NUMERIC,
        Schema::TYPE_SMALLINT => self::CATEGORY_NUMERIC,
        Schema::TYPE_INTEGER => self::CATEGORY_NUMERIC,
        Schema::TYPE_BIGINT => self::CATEGORY_NUMERIC,
        Schema::TYPE_FLOAT => self::CATEGORY_NUMERIC,
        Schema::TYPE_DOUBLE => self::CATEGORY_NUMERIC,
        Schema::TYPE_DECIMAL => self::CATEGORY_NUMERIC,
        Schema::TYPE_DATETIME => self::CATEGORY_TIME,
        Schema::TYPE_TIMESTAMP => self::CATEGORY_TIME,
        Schema::TYPE_TIME => self::CATEGORY_TIME,
        Schema::TYPE_DATE => self::CATEGORY_TIME,
        Schema::TYPE_BINARY => self::CATEGORY_OTHER,
        Schema::TYPE_BOOLEAN => self::CATEGORY_NUMERIC,
        Schema::TYPE_MONEY => self::CATEGORY_NUMERIC,
    ];

    public ?Connection $db;

    public string $comment;

    protected string $type;

    protected int|string|array $length;

    protected ?bool $isNotNull;

    protected bool $isUnique = false;

    protected string $check;

    protected ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null $default;

    protected $append;

    protected bool $isUnsigned = false;

    protected string $after;

    protected bool $isFirst;

    public function __construct(string $type, int|string|array $length = null, Connection $db = null)
    {
        $this->type = $type;
        $this->length = $length;
        $this->db = $db;
    }

    public function notNull(): self
    {
        $this->isNotNull = true;
        return $this;
    }

    public function unique(): self
    {
        $this->isUnique = true;
        return $this;
    }

    public function check(string $check): self
    {
        $this->check = $check;
        return $this;
    }

    public function defaultValue(ExpressionInterface|PdoValue|Query|string|bool|array|int|float|null $default): self
    {
        if ($default === null) {
            $this->null();
        }

        $this->default = $default;
        return $this;
    }

    public function null(): self
    {
        $this->isNotNull = false;
        return $this;
    }

    public function comment(string $comment): self
    {
        $this->comment = $comment;
        return $this;
    }

    public function unsigned(): self
    {
        switch ($this->type) {
            case Schema::TYPE_PK:
                $this->type = Schema::TYPE_UPK;
                break;
            case Schema::TYPE_BIGPK:
                $this->type = Schema::TYPE_UBIGPK;
                break;
        }
        $this->isUnsigned = true;
        return $this;
    }

    public function after(string $after): self
    {
        $this->after = $after;
        return $this;
    }

    public function first(): self
    {
        $this->isFirst = true;
        return $this;
    }

    public function defaultExpression(string $default): self
    {
        $this->default = new Expression($default);
        return $this;
    }

    public function append(string $sql): self
    {
        $this->append = $sql;
        return $this;
    }

    public function __toString()
    {
        switch ($this->getTypeCategory()) {
            case self::CATEGORY_PK:
                $format = '{type}{check}{comment}{append}';
                break;
            default:
                $format = '{type}{length}{notnull}{unique}{default}{check}{comment}{append}';
        }

        return $this->buildCompleteString($format);
    }

    protected function getTypeCategory(): ?string
    {
        return isset($this->categoryMap[$this->type]) ? $this->categoryMap[$this->type] : null;
    }

    protected function buildCompleteString(string $format): string
    {
        $placeholderValues = [
            '{type}' => $this->type,
            '{length}' => $this->buildLengthString(),
            '{unsigned}' => $this->buildUnsignedString(),
            '{notnull}' => $this->buildNotNullString(),
            '{unique}' => $this->buildUniqueString(),
            '{default}' => $this->buildDefaultString(),
            '{check}' => $this->buildCheckString(),
            '{comment}' => $this->buildCommentString(),
            '{pos}' => $this->isFirst ? $this->buildFirstString() : $this->buildAfterString(),
            '{append}' => $this->buildAppendString(),
        ];
        return strtr($format, $placeholderValues);
    }

    protected function buildLengthString(): string
    {
        if ($this->length === null || $this->length === []) {
            return '';
        }
        if (is_array($this->length)) {
            $this->length = implode(',', $this->length);
        }

        return "({$this->length})";
    }

    protected function buildUnsignedString(): string
    {
        return '';
    }

    protected function buildNotNullString(): string
    {
        if ($this->isNotNull === true) {
            return ' NOT NULL';
        } elseif ($this->isNotNull === false) {
            return ' NULL';
        }

        return '';
    }

    protected function buildUniqueString(): string
    {
        return $this->isUnique ? ' UNIQUE' : '';
    }

    protected function buildDefaultString(): string
    {
        if ($this->default === null) {
            return $this->isNotNull === false ? ' DEFAULT NULL' : '';
        }

        $string = ' DEFAULT ';
        switch (gettype($this->default)) {
            case 'object':
            case 'integer':
                $string .= (string)$this->default;
                break;
            case 'double':
                // ensure type cast always has . as decimal separator in all locales
                $string .= StringHelper::floatToString($this->default);
                break;
            case 'boolean':
                $string .= $this->default ? 'TRUE' : 'FALSE';
                break;
            default:
                $string .= "'{$this->default}'";
        }

        return $string;
    }

    protected function buildCheckString(): string
    {
        return $this->check !== null ? " CHECK ({$this->check})" : '';
    }

    protected function buildCommentString(): string
    {
        return '';
    }

    protected function buildFirstString(): string
    {
        return '';
    }

    protected function buildAfterString(): string
    {
        return '';
    }

    protected function buildAppendString(): string
    {
        return $this->append !== null ? ' ' . $this->append : '';
    }
}
