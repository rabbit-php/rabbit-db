<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

trait QueryTrait
{
    public null|string|array $where = null;

    public null|int|ExpressionInterface $limit = null;

    public null|int|ExpressionInterface $offset = null;

    public ?array $orderBy = null;

    public ?string $indexBy = null;

    public bool $emulateExecution = false;

    public function indexBy(string|callable $column): QueryInterface
    {
        $this->indexBy = $column;
        return $this;
    }

    public function filterWhere(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->where($condition);
        }

        return $this;
    }

    protected function filterCondition(array $condition): array
    {
        if (!is_array($condition)) {
            return $condition;
        }

        if (!isset($condition[0])) {
            // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
            foreach ($condition as $name => $value) {
                if ($this->isEmpty($value)) {
                    unset($condition[$name]);
                }
            }

            return $condition;
        }

        // operator format: operator, operand 1, operand 2, ...

        $operator = array_shift($condition);

        switch (strtoupper($operator)) {
            case 'NOT':
            case 'AND':
            case 'OR':
                foreach ($condition as $i => $operand) {
                    $subCondition = $this->filterCondition($operand);
                    if ($this->isEmpty($subCondition)) {
                        unset($condition[$i]);
                    } else {
                        $condition[$i] = $subCondition;
                    }
                }

                if (empty($condition)) {
                    return [];
                }
                break;
            case 'BETWEEN':
            case 'NOT BETWEEN':
                if (array_key_exists(1, $condition) && array_key_exists(2, $condition)) {
                    if ($this->isEmpty($condition[1]) || $this->isEmpty($condition[2])) {
                        return [];
                    }
                }
                break;
            default:
                if (array_key_exists(1, $condition) && $this->isEmpty($condition[1] = is_numeric($condition[1]) ? (string)$condition[1] : $condition[1])) {
                    return [];
                }
        }

        array_unshift($condition, $operator);

        return $condition;
    }

    protected function isEmpty(null|string|array $value): bool
    {
        return $value === '' || $value === [] || $value === null || is_string($value) && trim($value) === '';
    }

    public function where(string|array $condition): self
    {
        $this->where = $condition;
        return $this;
    }

    public function andFilterWhere(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->andWhere($condition);
        }

        return $this;
    }

    public function andWhere(string|array $condition): self
    {
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['and', $this->where, $condition];
        }

        return $this;
    }

    public function orFilterWhere(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->orWhere($condition);
        }

        return $this;
    }

    public function orWhere(string|array $condition): self
    {
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['or', $this->where, $condition];
        }

        return $this;
    }

    public function orderBy(string|array|ExpressionInterface $columns): self
    {
        $this->orderBy = $this->normalizeOrderBy($columns);
        return $this;
    }

    public function addOrderBy(string|array|ExpressionInterface $columns): self
    {
        $columns = $this->normalizeOrderBy($columns);
        if ($this->orderBy === null) {
            $this->orderBy = $columns;
        } else {
            $this->orderBy = [...$this->orderBy, ...$columns];
        }

        return $this;
    }

    public function limit(int|ExpressionInterface|null $limit): self
    {
        $this->limit = $limit;
        return $this;
    }

    public function offset(int|ExpressionInterface|null $offset): self
    {
        $this->offset = $offset;
        return $this;
    }

    public function emulateExecution(bool $value = true): self
    {
        $this->emulateExecution = $value;
        return $this;
    }
}
