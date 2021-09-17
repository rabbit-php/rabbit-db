<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\Core\BaseObject;
use Throwable;

class BatchQueryResult extends BaseObject implements \Iterator
{
    public Connection $db;
    public Query $query;
    public int $batchSize = 100;
    public bool $each = false;
    protected ?DataReader $dataReader = null;
    protected ?array $batch = null;
    protected ?array $value = null;
    protected ?int $key = null;


    /**
     * Destructor.
     */
    public function __destruct()
    {
        // make sure cursor is closed
        $this->reset();
    }

    /**
     * Resets the batch query.
     * This method will clean up the existing batch query so that a new batch query can be performed.
     */
    public function reset(): void
    {
        if ($this->dataReader !== null) {
            $this->dataReader->close();
        }
        $this->dataReader = null;
        $this->batch = null;
        $this->value = null;
        $this->key = null;
    }

    /**
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function rewind(): void
    {
        $this->reset();
        $this->next();
    }

    /**
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function next(): void
    {
        if ($this->batch === null || !$this->each || $this->each && next($this->batch) === false) {
            $this->batch = $this->fetchData();
            reset($this->batch);
        }

        if ($this->each) {
            $data = current($this->batch);
            $this->value = $data !== false ? $data : null;
            if ($this->query->indexBy !== null) {
                $this->key = key($this->batch);
            } elseif (key($this->batch) !== null) {
                $this->key = $this->key === null ? 0 : $this->key + 1;
            } else {
                $this->key = null;
            }
        } else {
            $this->value = $this->batch;
            $this->key = $this->key === null ? 0 : $this->key + 1;
        }
    }

    /**
     * @return array
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    protected function fetchData(): ?array
    {
        if ($this->dataReader === null) {
            $this->dataReader = $this->query->createCommand()->query();
        }

        $rows = [];
        $count = 0;
        while ($count++ < $this->batchSize && ($row = $this->dataReader->read())) {
            $rows[] = $row;
        }

        return $this->query->populate($rows);
    }

    /**
     * Returns the index of the current dataset.
     * This method is required by the interface [[\Iterator]].
     * @return int the index of the current row.
     */
    public function key(): int
    {
        return $this->key;
    }

    /**
     * Returns the current dataset.
     * This method is required by the interface [[\Iterator]].
     * @return mixed the current dataset.
     */
    public function current(): null|float|int|string|bool|array
    {
        return $this->value;
    }

    /**
     * Returns whether there is a valid dataset at the current position.
     * This method is required by the interface [[\Iterator]].
     * @return bool whether there is a valid dataset at the current position.
     */
    public function valid(): bool
    {
        return !empty($this->batch);
    }
}
