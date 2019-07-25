<?php


namespace rabbit\db;

/**
 * Interface RetryHandlerInterface
 * @package rabbit\db
 */
abstract class RetryHandlerInterface
{
    /** @var int */
    protected $totalCount;
    /**
     * @return int
     */
    abstract public function getTotalCount():int;
    /**
     * @param int $count
     */
    abstract public function setTotalCount(int $count): void;

    /**
     * @param Command $cmd
     * @param \Throwable $e
     * @param int $count
     * @return bool
     */
    abstract public function handle(ConnectionInterface $db, \Throwable $e, int $count): bool;
}