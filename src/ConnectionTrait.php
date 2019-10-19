<?php
/**
 * Created by PhpStorm.
 * User: Administrator
 * Date: 2018/10/29
 * Time: 10:57
 */

namespace rabbit\db;

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
     * @var int
     */
    protected $lastTime;

    /**
     * @var string
     */
    protected $connectionId;

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
     * @return int
     */
    public function getLastTime(): int
    {
        return $this->lastTime;
    }

    /**
     * Update last time
     */
    public function updateLastTime(): void
    {
        $this->lastTime = time();
    }

    /**
     * @return string
     */
    public function getConnectionId(): string
    {
        return $this->connectionId;
    }

    /**
     * @return PoolInterface
     */
    public function getPool(): PoolInterface
    {
        return $this->pool;
    }

    /**
     * @return bool
     */
    public function isRecv(): bool
    {
        return $this->recv;
    }

    /**
     * @param bool $recv
     */
    public function setRecv(bool $recv): void
    {
        $this->recv = $recv;
    }

    /**
     * @param bool $release
     */
    public function release($release = false): void
    {
        if ($this->isAutoRelease() || $release) {
            PoolManager::getPool($this->poolKey)->release($this);
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
     * @param bool $defer
     * @throws NotSupportedException
     */
    public function setDefer($defer = true): bool
    {
        throw new NotSupportedException('can not call ' . __METHOD__);
    }
}
