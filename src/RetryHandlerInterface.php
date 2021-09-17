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
    const RETRY_NO = 0;
    const RETRY_CONNECT = 1;
    const RETRY_NOCONNECT = 2;

    protected int $totalCount;

    abstract public function getTotalCount(): int;

    abstract public function setTotalCount(int $count): void;

    abstract public function handle(Throwable $e, int $count): int;
}
