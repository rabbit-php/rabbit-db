<?php

declare(strict_types=1);

namespace Rabbit\DB;

use Rabbit\Base\App;
use Rabbit\Base\Core\Context;
use Rabbit\Pool\PoolInterface;
use Rabbit\Pool\PoolManager;

/**
 * Trait ConnectionTrait
 * @package Rabbit\DB
 */
trait ConnectionTrait
{
    protected string $poolKey;

    protected bool $autoRelease = true;

    protected bool $recv = true;

    public function createConnection(): void
    {
    }

    public function getPoolKey(): string
    {
        return $this->poolKey;
    }

    public function getPool(): ?PoolInterface
    {
        return PoolManager::getPool($this->poolKey);
    }

    public function release(bool $release = false): void
    {
        if (null !== $conn = DbContext::get($this->poolKey)) {
            $transaction = $this->getTransaction();
            if (!empty($transaction) && $transaction->getIsActive()) { //事务里面不释放连接
                return;
            }
            if ($this->autoRelease || $release) {
                $this->getPool()->release($conn);
                DbContext::delete($this->poolKey);
            }
        }
    }

    public function setInsertId(object $conn = null): void
    {
        $conn = $conn ?? DbContext::get($this->poolKey);
        if ($conn !== null) {
            Context::set($this->poolKey . '.id', $conn->lastInsertId());
        }
    }

    public function isAutoRelease(): bool
    {
        return $this->autoRelease;
    }

    public function setAutoRelease(bool $autoRelease): void
    {
        $this->autoRelease = $autoRelease;
    }

    public function reconnect(int $attempt = 0): void
    {
        DbContext::delete($this->poolKey);
        $this->getPool()->sub();
        App::warning("The $attempt times to Reconnect DB connection: " . $this->shortDsn, 'db');
        $this->open($attempt);
    }
}
