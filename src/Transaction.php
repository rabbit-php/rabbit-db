<?php

declare(strict_types=1);

namespace Rabbit\DB;

use PDO;
use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Exception\NotSupportedException;
use Throwable;

class Transaction extends BaseObject
{
    const READ_UNCOMMITTED = 'READ UNCOMMITTED';

    const READ_COMMITTED = 'READ COMMITTED';

    const REPEATABLE_READ = 'REPEATABLE READ';

    const SERIALIZABLE = 'SERIALIZABLE';

    protected int $_level = 0;

    public function __construct(public readonly Connection $db)
    {
    }

    public function begin(?string $isolationLevel = null): void
    {
        if (!$this->db->canTransaction) {
            return;
        }
        $this->db->open();

        $pdo = $this->db->getConn();
        if ($this->_level === 0) {
            if ($isolationLevel !== null) {
                $this->db->getSchema()->setTransactionIsolationLevel($isolationLevel);
            }
            App::debug('Begin transaction' . ($isolationLevel ? ' with isolation level ' . $isolationLevel : ''));
            $attempt = 0;
            while (true) {
                $attempt++;
                try {
                    $pdo->beginTransaction();
                    break;
                } catch (Throwable $e) {
                    if (($retryHandler = $this->db->getRetryHandler()) === null || (RetryHandlerInterface::RETRY_NO === $code = $retryHandler->handle($e, $attempt))) {
                        throw $e;
                    }
                    if ($code === RetryHandlerInterface::RETRY_CONNECT) {
                        $this->db->reconnect($attempt);
                        $pdo = $this->db->getConn();
                    }
                }
            }
            $this->_level = 1;
            return;
        }

        $schema = $this->db->getSchema();
        if ($schema->supportsSavepoint()) {
            App::debug('Set savepoint ' . $this->_level);
            try {
                if (!$pdo->getAttribute(PDO::ATTR_EMULATE_PREPARES)) {
                    $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
                    $schema->createSavepoint('LEVEL' . $this->_level);
                    $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
                } else {
                    $schema->createSavepoint('LEVEL' . $this->_level);
                }
            } catch (Throwable $e) {
                $this->db->close();
                throw $e;
            }
        } else {
            App::info('Transaction not started: nested transaction not supported');
            throw new NotSupportedException('Transaction not started: nested transaction not supported.');
        }
        $this->_level++;
    }

    public function commit(): void
    {
        if (!$this->db->canTransaction) {
            return;
        }
        if (!$this->getIsActive()) {
            $this->db->release(true);
            throw new Exception('Failed to commit transaction: transaction was inactive.');
        }

        $this->_level--;
        $pdo = $this->db->getConn();
        if ($this->_level === 0) {
            App::debug('Commit transaction');
            try {
                $pdo->inTransaction() && $pdo->commit();
            } catch (Throwable $e) {
                $this->db->close();
                throw $e;
            }

            $this->db->release(true);
            return;
        }

        $schema = $this->db->getSchema();
        if ($schema->supportsSavepoint()) {
            App::debug('Release savepoint ' . $this->_level);
            try {
                if (!$pdo->getAttribute(PDO::ATTR_EMULATE_PREPARES)) {
                    $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
                    $schema->releaseSavepoint('LEVEL' . $this->_level);
                    $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
                } else {
                    $schema->releaseSavepoint('LEVEL' . $this->_level);
                }
            } catch (Throwable $e) {
                $this->db->close();
                throw $e;
            }
        } else {
            App::info('Transaction not committed: nested transaction not supported');
        }
    }

    public function getIsActive(): bool
    {
        return $this->_level > 0;
    }

    public function rollBack(): void
    {
        if (!$this->db->canTransaction) {
            return;
        }
        if (!$this->getIsActive()) {
            $this->db->release(true);
            return;
        }

        $this->_level--;
        $pdo = $this->db->getConn();
        if ($this->_level === 0) {
            App::debug('Roll back transaction');
            try {
                $pdo->inTransaction() && $pdo->rollBack();
            } catch (Throwable $e) {
                $this->db->close();
                throw $e;
            }

            $this->db->release(true);
            return;
        }

        $schema = $this->db->getSchema();
        if ($schema->supportsSavepoint()) {
            App::debug('Roll back to savepoint ' . $this->_level);
            try {
                if (!$pdo->getAttribute(PDO::ATTR_EMULATE_PREPARES)) {
                    $pdo->getConn()->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
                    $schema->rollBackSavepoint('LEVEL' . $this->_level);
                    $pdo->getConn()->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
                } else {
                    $schema->rollBackSavepoint('LEVEL' . $this->_level);
                }
            } catch (Throwable $e) {
                $this->db->close();
                throw $e;
            }
        } else {
            App::info('Transaction not rolled back: nested transaction not supported');
        }
    }

    public function setIsolationLevel(string $level): void
    {
        if (!$this->db->canTransaction) {
            return;
        }
        if (!$this->getIsActive()) {
            $this->db->release(true);
            throw new Exception('Failed to set isolation level: transaction was inactive.');
        }
        App::debug('Setting transaction isolation level to ' . $level);
        $this->db->getSchema()->setTransactionIsolationLevel($level);
    }

    public function getLevel(): int
    {
        return $this->_level;
    }
}
