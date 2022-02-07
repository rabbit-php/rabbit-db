<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

class JsonExpression implements ExpressionInterface, \JsonSerializable
{
    const TYPE_JSON = 'json';
    const TYPE_JSONB = 'jsonb';

    protected string|array|object $value;

    protected ?string $type;

    public function __construct(string|array|object $value, string $type = null)
    {
        if ($value instanceof self) {
            $value = $value->getValue();
        }

        $this->value = $value;
        $this->type = $type;
    }

    public function getValue(): string|array|object
    {
        return $this->value;
    }

    public function getType(): ?string
    {
        return $this->type;
    }

    public function jsonSerialize(): mixed
    {
        $value = $this->getValue();
        if ($value instanceof QueryInterface) {
            throw new \InvalidArgumentException('The JsonExpression class can not be serialized to JSON when the value is a QueryInterface object');
        }

        return $value;
    }
}
