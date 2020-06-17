<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 10:57
 */

namespace rabbit\db;

use rabbit\App;
use rabbit\core\Context;
use rabbit\exception\NotSupportedException;
use rabbit\pool\PoolInterface;
use rabbit\pool\PoolManager;

/**
 * Trait ConnectionTrait
 * @package rabbit\db
 */
trait ConnectionTrait
{
    /**
     * @var string
     */
    protected $poolKey;

    /**
     * @var bool
     */
    protected $autoRelease = true;

    /**
     * Whether or not the package has been recv,default true
     *
     * @var bool
     */
    protected $recv = true;

    /**
     * @throws Exception
     */
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
            if ($this->isAutoRelease() || $release) {
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
     * @throws \Exception
     */
    public function reconnect(int $attempt = 0): void
    {
        DbContext::delete($this->poolName, $this->driver);
        $this->getPool()->sub();
        App::warning("The $attempt times to Reconnect DB connection: " . $this->shortDsn, 'db');
        $this->open($attempt);
    }
}
