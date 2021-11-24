<?php

declare(strict_types=1);

namespace Rabbit\DB;

use PDO;
use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Exception\NotSupportedException;

class Transaction extends BaseObject
{
    const READ_UNCOMMITTED = 'READ UNCOMMITTED';

    const READ_COMMITTED = 'READ COMMITTED';

    const REPEATABLE_READ = 'REPEATABLE READ';

    const SERIALIZABLE = 'SERIALIZABLE';

    public ?Connection $db;

    protected int $_level = 0;

    public function __construct(Connection $db)
    {
        $this->db = $db;
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
            App::debug('Begin transaction' . ($isolationLevel ? ' with isolation level ' . $isolationLevel : ''), "db");
            $pdo->beginTransaction();
            $this->_level = 1;
            return;
        }

        $schema = $this->db->getSchema();
        if ($schema->supportsSavepoint()) {
            App::debug('Set savepoint ' . $this->_level, "db");
            if (!$pdo->getAttribute(PDO::ATTR_EMULATE_PREPARES)) {
                $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
                $schema->createSavepoint('LEVEL' . $this->_level);
                $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
            } else {
                $schema->createSavepoint('LEVEL' . $this->_level);
            }
        } else {
            App::info('Transaction not started: nested transaction not supported', "db");
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
            App::debug('Commit transaction', "db");
            $pdo->inTransaction() && $pdo->commit();
            $this->db->release(true);
            return;
        }

        $schema = $this->db->getSchema();
        if ($schema->supportsSavepoint()) {
            App::debug('Release savepoint ' . $this->_level, "db");
            if (!$pdo->getAttribute(PDO::ATTR_EMULATE_PREPARES)) {
                $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
                $schema->releaseSavepoint('LEVEL' . $this->_level);
                $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
            } else {
                $schema->releaseSavepoint('LEVEL' . $this->_level);
            }
        } else {
            App::info('Transaction not committed: nested transaction not supported', "db");
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
            App::debug('Roll back transaction', "db");
            $pdo->inTransaction() && $pdo->rollBack();
            $this->db->release(true);
            return;
        }

        $schema = $this->db->getSchema();
        if ($schema->supportsSavepoint()) {
            App::debug('Roll back to savepoint ' . $this->_level, "db");
            if (!$pdo->getAttribute(PDO::ATTR_EMULATE_PREPARES)) {
                $pdo->getConn()->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
                $schema->rollBackSavepoint('LEVEL' . $this->_level);
                $pdo->getConn()->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
            } else {
                $schema->rollBackSavepoint('LEVEL' . $this->_level);
            }
        } else {
            App::info('Transaction not rolled back: nested transaction not supported', "db");
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
        App::debug('Setting transaction isolation level to ' . $level, "db");
        $this->db->getSchema()->setTransactionIsolationLevel($level);
    }

    public function getLevel(): int
    {
        return $this->_level;
    }
}
