<?php

declare(strict_types=1);

namespace Rabbit\DB;

use Rabbit\Base\Exception\InvalidConfigException;
use Traversable;

class ArrayExpression implements ExpressionInterface, \ArrayAccess, \Countable, \IteratorAggregate
{
    private ExpressionInterface|array $value;

    public function __construct(null|ExpressionInterface|array $value = [], private ?string $type = null, private int $dimension = 1)
    {
        if ($value instanceof self) {
            $value = $value->getValue();
        }

        $this->value = $value;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @return string|null
     */
    public function getType(): ?string
    {
        return $this->type;
    }


    public function getValue()
    {
        return $this->value;
    }

    public function getDimension(): int
    {
        return $this->dimension;
    }

    public function offsetExists($offset): bool
    {
        return isset($this->value[$offset]);
    }

    public function offsetGet($offset)
    {
        return $this->value[$offset];
    }

    public function offsetSet($offset, $value): void
    {
        $this->value[$offset] = $value;
    }

    public function offsetUnset($offset): void
    {
        unset($this->value[$offset]);
    }

    public function count(): int
    {
        return \count($this->value);
    }

    public function getIterator(): Traversable
    {
        $value = $this->getValue();
        if ($value instanceof QueryInterface) {
            throw new InvalidConfigException(
                'The ArrayExpression class can not be iterated when the value is a QueryInterface object'
            );
        }
        if ($value === null) {
            $value = [];
        }

        return new \ArrayIterator($value);
    }
}
