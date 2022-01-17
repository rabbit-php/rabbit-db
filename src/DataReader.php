<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Rabbit\Base\Exception\InvalidCallException;

final class DataReader implements \Iterator, \Countable
{
    protected ?\PDOStatement $statement;
    protected bool $closed = false;
    protected $row;
    protected int $index = -1;

    public function __construct(protected Command $command, array $config = [])
    {
        $this->statement = $command->pdoStatement;
        $this->statement->setFetchMode(\PDO::FETCH_ASSOC);

        configure($this, $config);
    }

    public function bindColumn(int|string $column, &$value, ?int $dataType = null): void
    {
        if ($dataType === null) {
            $this->statement->bindColumn($column, $value);
        } else {
            $this->statement->bindColumn($column, $value, $dataType);
        }
    }

    public function setFetchMode(int $mode): void
    {
        $params = func_get_args();
        call_user_func_array([$this->statement, 'setFetchMode'], $params);
    }

    public function read(): ?array
    {
        if (false === $row = $this->statement->fetch()) {
            return null;
        }
        return $row;
    }

    public function readColumn(int $columnIndex): null|string|int|float|bool|array
    {
        return $this->statement->fetchColumn($columnIndex);
    }

    public function readObject(string $className, array $fields): null|string|int|float|bool|object
    {
        return $this->statement->fetchObject($className, $fields);
    }

    public function readAll(): array
    {
        return $this->statement->fetchAll();
    }

    public function nextResult(): null|string|int|float|bool|object
    {
        if (($result = $this->statement->nextRowset()) !== false) {
            $this->index = -1;
        }

        return $result;
    }

    public function close(): void
    {
        $this->statement->closeCursor();
        $this->closed = true;
    }

    public function getIsClosed(): bool
    {
        return $this->closed;
    }

    public function count(): int
    {
        return $this->getRowCount();
    }

    public function getRowCount(): int
    {
        return $this->statement->rowCount();
    }

    public function getColumnCount(): int
    {
        return $this->statement->columnCount();
    }

    public function rewind()
    {
        if ($this->index < 0) {
            $this->row = $this->statement->fetch();
            $this->index = 0;
        } else {
            throw new InvalidCallException('DataReader cannot rewind. It is a forward-only reader.');
        }
    }

    public function key()
    {
        return $this->index;
    }

    public function current()
    {
        return $this->row;
    }

    public function next()
    {
        $this->row = $this->statement->fetch();
        $this->index++;
    }

    public function valid()
    {
        return $this->row !== false;
    }
}
