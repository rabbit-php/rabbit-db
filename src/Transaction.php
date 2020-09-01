<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use PDO;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Exception\NotSupportedException;
use Throwable;

/**
 * Transaction represents a DB transaction.
 *
 * It is usually created by calling [[Connection::beginTransaction()]].
 *
 * The following code is a typical example of using transactions (note that some
 * DBMS may not support transactions):
 *
 * ```php
 * $transaction = $connection->beginTransaction();
 * try {
 *     $connection->createCommand($sql1)->execute();
 *     $connection->createCommand($sql2)->execute();
 *     //.... other SQL executions
 *     $transaction->commit();
 * } catch (\Throwable $e) {
 *     $transaction->rollBack();
 *     throw $e;
 * }
 * ```
 *
 * @property bool $isActive Whether this transaction is active. Only an active transaction can [[commit()]] or
 * [[rollBack()]]. This property is read-only.
 * @property string $isolationLevel The transaction isolation level to use for this transaction. This can be
 * one of [[READ_UNCOMMITTED]], [[READ_COMMITTED]], [[REPEATABLE_READ]] and [[SERIALIZABLE]] but also a string
 * containing DBMS specific syntax to be used after `SET TRANSACTION ISOLATION LEVEL`. This property is
 * write-only.
 * @property int $level The current nesting level of the transaction. This property is read-only.
 *
 * @author Qiang Xue <qiang.xue@gmail.com>
 * @since 2.0
 */
class Transaction extends BaseObject
{
    /**
     * A constant representing the transaction isolation level `READ UNCOMMITTED`.
     * @see http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Isolation_levels
     */
    const READ_UNCOMMITTED = 'READ UNCOMMITTED';
    /**
     * A constant representing the transaction isolation level `READ COMMITTED`.
     * @see http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Isolation_levels
     */
    const READ_COMMITTED = 'READ COMMITTED';
    /**
     * A constant representing the transaction isolation level `REPEATABLE READ`.
     * @see http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Isolation_levels
     */
    const REPEATABLE_READ = 'REPEATABLE READ';
    /**
     * A constant representing the transaction isolation level `SERIALIZABLE`.
     * @see http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Isolation_levels
     */
    const SERIALIZABLE = 'SERIALIZABLE';

    /**
     * @var Connection the database connection that this transaction is associated with.
     */
    public ?Connection $db;

    /**
     * @var int the nesting level of the transaction. 0 means the outermost level.
     */
    protected int $_level = 0;


    public function __construct(Connection $db)
    {
        $this->db = $db;
    }

    /**
     * Begins a transaction.
     * @param string|null $isolationLevel The [isolation level][] to use for this transaction.
     * This can be one of [[READ_UNCOMMITTED]], [[READ_COMMITTED]], [[REPEATABLE_READ]] and [[SERIALIZABLE]] but
     * also a string containing DBMS specific syntax to be used after `SET TRANSACTION ISOLATION LEVEL`.
     * If not specified (`null`) the isolation level will not be set explicitly and the DBMS default will be used.
     *
     * > Note: This setting does not work for PostgreSQL, where setting the isolation level before the transaction
     * has no effect. You have to call [[setIsolationLevel()]] in this case after the transaction has started.
     *
     * > Note: Some DBMS allow setting of the isolation level only for the whole connection so subsequent transactions
     * may get the same isolation level even if you did not specify any. When using this feature
     * you may need to set the isolation level for all transactions explicitly to avoid conflicting settings.
     * At the time of this writing affected DBMS are MSSQL and SQLite.
     *
     * [isolation level]: http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Isolation_levels
     *
     * Starting from version 2.0.16, this method throws exception when beginning nested transaction and underlying DBMS
     * does not support savepoints.
     * @throws NotSupportedException if the DBMS does not support nested transactions
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function begin(?string $isolationLevel = null): void
    {
        if ($this->db === null) {
            throw new \InvalidArgumentException('Transaction::db must be set.');
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
                $pdo->getConn()->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
                $schema->createSavepoint('LEVEL' . $this->_level);
                $pdo->getConn()->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
            } else {
                $schema->createSavepoint('LEVEL' . $this->_level);
            }
        } else {
            App::info('Transaction not started: nested transaction not supported', "db");
            throw new NotSupportedException('Transaction not started: nested transaction not supported.');
        }
        $this->_level++;
    }

    /**
     * Commits a transaction.
     * @throws Exception if the transaction is not active
     * @throws NotSupportedException
     * @throws Throwable|InvalidArgumentException
     */
    public function commit(): void
    {
        if (!$this->getIsActive()) {
            throw new Exception('Failed to commit transaction: transaction was inactive.');
        }

        $this->_level--;
        $pdo = $this->db->getConn();
        if ($this->_level === 0) {
            App::debug('Commit transaction', "db");
            $pdo->commit();
            $this->db->release(true);
            return;
        }

        $schema = $this->db->getSchema();
        if ($schema->supportsSavepoint()) {
            App::debug('Release savepoint ' . $this->_level, "db");
            if (!$pdo->getAttribute(PDO::ATTR_EMULATE_PREPARES)) {
                $pdo->getConn()->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);
                $schema->releaseSavepoint('LEVEL' . $this->_level);
                $pdo->getConn()->setAttribute(PDO::ATTR_EMULATE_PREPARES, false);
            } else {
                $schema->releaseSavepoint('LEVEL' . $this->_level);
            }
        } else {
            App::info('Transaction not committed: nested transaction not supported', "db");
        }
    }

    /**
     * Returns a value indicating whether this transaction is active.
     * @return bool whether this transaction is active. Only an active transaction
     * can [[commit()]] or [[rollBack()]].
     */
    public function getIsActive(): bool
    {
        return $this->_level > 0 && $this->db && $this->db->getIsActive();
    }

    /**
     * Rolls back a transaction.
     * @throws Throwable|InvalidArgumentException
     */
    public function rollBack(): void
    {
        if (!$this->getIsActive()) {
            // do nothing if transaction is not active: this could be the transaction is committed
            // but the event handler to "commitTransaction" throw an exception
            return;
        }

        $this->_level--;
        $pdo = $this->db->getConn();
        if ($this->_level === 0) {
            App::debug('Roll back transaction', "db");
            $pdo->rollBack();
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

    /**
     * Sets the transaction isolation level for this transaction.
     *
     * This method can be used to set the isolation level while the transaction is already active.
     * However this is not supported by all DBMS so you might rather specify the isolation level directly
     * when calling [[begin()]].
     * @param string $level The transaction isolation level to use for this transaction.
     * This can be one of [[READ_UNCOMMITTED]], [[READ_COMMITTED]], [[REPEATABLE_READ]] and [[SERIALIZABLE]] but
     * also a string containing DBMS specific syntax to be used after `SET TRANSACTION ISOLATION LEVEL`.
     * @throws Exception if the transaction is not active
     * @throws NotSupportedException
     * @throws Throwable|InvalidArgumentException
     * @see http://en.wikipedia.org/wiki/Isolation_%28database_systems%29#Isolation_levels
     */
    public function setIsolationLevel(string $level): void
    {
        if (!$this->getIsActive()) {
            throw new Exception('Failed to set isolation level: transaction was inactive.');
        }
        App::debug('Setting transaction isolation level to ' . $level, "db");
        $this->db->getSchema()->setTransactionIsolationLevel($level);
    }

    /**
     * @return int The current nesting level of the transaction.
     * @since 2.0.8
     */
    public function getLevel(): int
    {
        return $this->_level;
    }
}
