<?php
declare(strict_types=1);

namespace Rabbit\DB;

use Throwable;

/**
 * Class RetryHandlerInterface
 * @package Rabbit\DB
 */
abstract class RetryHandlerInterface
{
    /** @var int */
    protected int $totalCount;
    /**
     * @return int
     */
    abstract public function getTotalCount():int;
    /**
     * @param int $count
     */
    abstract public function setTotalCount(int $count): void;

    /**
     * @param Throwable $e
     * @param int $count
     * @return bool
     */
    abstract public function handle(Throwable $e, int $count): bool;
}
