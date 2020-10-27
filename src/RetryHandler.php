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
    /** @var int */
    protected int $sleep = 1;

    /**
     * RetryHandler constructor.
     * @param int $totalCount
     */
    public function __construct(int $totalCount = 3)
    {
        $this->totalCount = $totalCount;
    }

    /**
     * @return int
     */
    public function getTotalCount(): int
    {
        return $this->totalCount;
    }

    /**
     * @param int $count
     */
    public function setTotalCount(int $count): void
    {
        $this->totalCount = $count;
    }

    /**
     * @param Throwable $e
     * @param int $count
     * @return bool
     */
    public function handle(Throwable $e, int $count): bool
    {
        if ($count < $this->totalCount) {
            $count > 1 && \Co::sleep($this->sleep);
            return true;
        }
        return false;
    }
}
