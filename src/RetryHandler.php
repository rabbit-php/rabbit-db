<?php

declare(strict_types=1);

namespace Rabbit\DB;

use Throwable;

/**
 * Class RetryHandler
 * @package Rabbit\DB
 */
class RetryHandler extends RetryHandlerInterface
{
    protected int $sleep = 1;

    public function __construct(int $totalCount = 3)
    {
        $this->totalCount = $totalCount;
    }

    public function getTotalCount(): int
    {
        return $this->totalCount;
    }

    public function setTotalCount(int $count): void
    {
        $this->totalCount = $count;
    }

    public function handle(Throwable $e, int $count): int
    {
        if ($count < $this->totalCount) {
            $count > 1 && sleep($this->sleep);
            return static::RETRY_CONNECT;
        }
        return static::RETRY_NO;
    }
}
