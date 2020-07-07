<?php
declare(strict_types=1);

namespace Rabbit\DB;

/**
 * Interface BatchInterface
 * @package Rabbit\DB
 */
interface BatchInterface
{
    /**
     * @param array $columns
     * @return bool
     */
    public function addColumns(array $columns): bool;

    /**
     * @param array $rows
     * @param bool $checkFields
     * @return bool
     */
    public function addRow(array $rows, bool $checkFields = true): bool;

    /**
     * @return mixed
     */
    public function execute(): int;
}