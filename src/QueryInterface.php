<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

interface QueryInterface
{
    public function all(): array;

    public function one(): ?array;

    public function count(string $q = '*'): int;

    public function exists(): bool;

    public function indexBy(string|callable $column): self;

    public function where(string|array $condition): self;

    public function andWhere(string|array $condition): self;

    public function orWhere(string|array $condition): self;

    public function filterWhere(array $condition): self;

    public function andFilterWhere(array $condition): self;

    public function orFilterWhere(array $condition): self;

    public function orderBy(string|array $columns): self;

    public function addOrderBy(string|array $columns): self;

    public function limit(int|null $limit): self;

    public function offset(int|null $offset): self;

    public function emulateExecution(bool $value = true): self;

    public function totals(): ?int;
}
