<?php


namespace rabbit\db;

/**
 * Interface RetryHandlerInterface
 * @package rabbit\db
 */
interface RetryHandlerInterface
{
    /**
     * @param int $count
     */
    public function setTotalCount(int $count): void;

    /**
     * @param Command $cmd
     * @param \Throwable $e
     * @param int $count
     * @return bool
     */
    public function handle(Command $cmd, \Throwable $e, int $count): bool;
}