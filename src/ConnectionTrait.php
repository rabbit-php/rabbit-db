<?php
declare(strict_types=1);

namespace Rabbit\DB;

use Rabbit\Base\App;
use Rabbit\Base\Core\Context;
use Rabbit\Pool\PoolInterface;
use Rabbit\Pool\PoolManager;
use Throwable;

/**
 * Trait ConnectionTrait
 * @package Rabbit\DB
 */
trait ConnectionTrait
{
    /**
     * @var string
     */
    protected string $poolKey;

    /**
     * @var bool
     */
    protected bool $autoRelease = true;

    /**
     * Whether or not the package has been recv,default true
     *
     * @var bool
     */
    protected bool $recv = true;

    public function createConnection(): void
    {
    }

    /**
     * @return PoolInterface
     */
    public function getPool(): PoolInterface
    {
        return PoolManager::getPool($this->poolKey);
    }

    /**
     * @param bool $release
     */
    public function release($release = false): void
    {
        if (null !== $conn = DbContext::get($this->poolName, $this->driver)) {
            $transaction = $this->getTransaction();
            if (!empty($transaction) && $transaction->getIsActive()) {//事务里面不释放连接
                return;
            }
            if ($this->autoRelease || $release) {
                $this->getPool()->release($conn);
                DbContext::delete($this->poolName, $this->driver);
            }
        }
    }

    /**
     * @param $conn
     */
    public function setInsertId($conn = null): void
    {
        $conn = $conn ?? DbContext::get($this->poolName, $this->driver);
        if ($conn !== null) {
            Context::set($this->poolName . '.id', $conn->lastInsertId());
        }
    }

    /**
     * @return bool
     */
    public function isAutoRelease(): bool
    {
        return $this->autoRelease;
    }

    /**
     * @param bool $autoRelease
     */
    public function setAutoRelease(bool $autoRelease): void
    {
        $this->autoRelease = $autoRelease;
    }

    /**
     * @param int $attempt
     * @throws Throwable
     */
    public function reconnect(int $attempt = 0): void
    {
        DbContext::delete($this->poolName, $this->driver);
        $this->getPool()->sub();
        App::warning("The $attempt times to Reconnect DB connection: " . $this->shortDsn, 'db');
        $this->open($attempt);
    }
}
