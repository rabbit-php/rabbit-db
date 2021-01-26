<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use DI\DependencyException;
use DI\NotFoundException;
use PDO;
use Psr\SimpleCache\CacheInterface;
use Psr\SimpleCache\InvalidArgumentException;
use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\UrlHelper;
use Rabbit\Cache\ArrayCache;
use ReflectionException;
use Throwable;

/**
 * Connection represents a connection to a database via [PDO](http://php.net/manual/en/book.pdo.php).
 *
 * Connection works together with [[Command]], [[DataReader]] and [[Transaction]]
 * to provide data access to various DBMS in a common set of APIs. They are a thin wrapper
 * of the [PDO PHP extension](http://php.net/manual/en/book.pdo.php).
 *
 * Connection supports database replication and read-write splitting. In particular, a Connection component
 * can be configured with multiple [[masters]] and [[slaves]]. It will do load balancing and failover by choosing
 * appropriate servers. It will also automatically direct read operations to the slaves and write operations to
 * the masters.
 *
 * To establish a DB connection, set [[dsn]], [[username]] and [[password]], and then
 * call [[open()]] to connect to the database server. The current state of the connection can be checked using [[$isActive]].
 *
 * The following example shows how to create a Connection instance and establish
 * the DB connection:
 *
 * ```php
 * $connection = new \rabbit\db\Connection([
 *     'dsn' => $dsn,
 *     'username' => $username,
 *     'password' => $password,
 * ]);
 * $connection->open();
 * ```
 *
 * After the DB connection is established, one can execute SQL statements like the following:
 *
 * ```php
 * $command = $connection->createCommand('SELECT * FROM post');
 * $posts = $command->queryAll();
 * $command = $connection->createCommand('UPDATE post SET status=1');
 * $command->execute();
 * ```
 *
 * One can also do prepared SQL execution and bind parameters to the prepared SQL.
 * When the parameters are coming from user input, you should use this approach
 * to prevent SQL injection attacks. The following is an example:
 *
 * ```php
 * $command = $connection->createCommand('SELECT * FROM post WHERE id=:id');
 * $command->bindValue(':id', $_GET['id']);
 * $post = $command->query();
 * ```
 *
 * For more information about how to perform various DB queries, please refer to [[Command]].
 *
 * If the underlying DBMS supports transactions, you can perform transactional SQL queries
 * like the following:
 *
 * ```php
 * $transaction = $connection->beginTransaction();
 * try {
 *     $connection->createCommand($sql1)->execute();
 *     $connection->createCommand($sql2)->execute();
 *     // ... executing other SQL statements ...
 *     $transaction->commit();
 * } catch (Exception $e) {
 *     $transaction->rollBack();
 * }
 * ```
 *
 * You also can use shortcut for the above like the following:
 *
 * ```php
 * $connection->transaction(function () {
 *     $order = new Order($customer);
 *     $order->save();
 *     $order->addItems($items);
 * });
 * ```
 *
 * If needed you can pass transaction isolation level as a second parameter:
 *
 * ```php
 * $connection->transaction(function (Connection $db) {
 *     //return $db->...
 * }, Transaction::READ_UNCOMMITTED);
 * ```
 *
 * Connection is often used as an application component and configured in the application
 * configuration like the following:
 *
 * ```php
 * 'components' => [
 *     'db' => [
 *         'class' => \rabbit\db\Connection::class,
 *         'dsn' => 'mysql:host=127.0.0.1;dbname=demo;charset=utf8',
 *         'username' => 'root',
 *         'password' => '',
 *     ],
 * ],
 * ```
 *
 * The [[dsn]] property can be defined via configuration array:
 *
 * ```php
 * 'components' => [
 *     'db' => [
 *         'class' => \rabbit\db\Connection::class,
 *         'dsn' => [
 *             'driver' => 'mysql',
 *             'host' => '127.0.0.1',
 *             'dbname' => 'demo',
 *             'charset' => 'utf8',
 *          ],
 *         'username' => 'root',
 *         'password' => '',
 *     ],
 * ],
 * ```
 *
 * @property string $driverName Name of the DB driver.
 * @property bool $isActive Whether the DB connection is established. This property is read-only.
 * @property string $lastInsertID The row ID of the last row inserted, or the last value retrieved from the
 * sequence object. This property is read-only.
 * @property Connection $master The currently active master connection. `null` is returned if there is no
 * master available. This property is read-only.
 * @property PDO $masterPdo The PDO instance for the currently active master connection. This property is
 * read-only.
 * @property QueryBuilder $queryBuilder The query builder for the current DB connection. Note that the type of
 * this property differs in getter and setter. See [[getQueryBuilder()]] and [[setQueryBuilder()]] for details.
 * @property string $serverVersion Server version as a string. This property is read-only.
 * @property Connection $slave The currently active slave connection. `null` is returned if there is no slave
 * available and `$fallbackToMaster` is false. This property is read-only.
 * @property PDO $slavePdo The PDO instance for the currently active slave connection. `null` is returned if
 * no slave connection is available and `$fallbackToMaster` is false. This property is read-only.
 * @property Transaction|null $transaction The currently active transaction. Null if no active transaction.
 * This property is read-only.
 *
 * @author Qiang Xue <qiang.xue@gmail.com>
 * @since 2.0
 */
class Connection extends BaseObject implements ConnectionInterface
{
    use ConnectionTrait;

    public ?string $dsn;
    public ?string $shortDsn;
    public ?string $username = null;
    public ?string $password = null;
    public ?array $attributes = null;
    public bool $enableSchemaCache = true;
    public ?int $schemaCacheDuration = null;
    public array $schemaCacheExclude = [];
    public CacheInterface $schemaCache;
    public bool $enableQueryCache = true;
    public ?CacheInterface $queryCache = null;
    public ?string $charset = null;
    public ?bool $emulatePrepare = null;
    public string $tablePrefix = '';
    public array $schemaMap = [];
    public string $pdoClass = 'PDO';
    public bool $enableSavepoint = true;
    public CacheInterface $serverStatusCache;
    public int $serverRetryInterval = 600;
    public bool $enableSlaves = true;
    public array $slaves = [];
    public array $slaveConfig = [];
    public array $masters = [];
    public array $masterConfig = [];
    public bool $shuffleMasters = true;
    public bool $enableLogging = true;
    public int $maxLog = 1024;
    protected string $transactionClass = Transaction::class;
    protected ?Schema $schema = null;
    protected ?string $_driverName = null;
    protected ?Connection $master = null;
    protected ?Connection $slave = null;
    protected ?RetryHandlerInterface $retryHandler = null;
    protected string $commandClass = Command::class;
    public string $poolName = 'default';
    public string $driver = 'pdo';
    protected ?array $parseDsn = [];

    /**
     * Connection constructor.
     * @param string $dsn
     */
    public function __construct(string $dsn)
    {
        $this->dsn = $dsn;
        $this->schemaCache = new ArrayCache();
        $this->serverStatusCache = new ArrayCache();
        $this->makeShortDsn();
    }

    /**
     * @return mixed|null
     */
    public function getConn()
    {
        return DbContext::get($this->poolName, $this->driver);
    }

    /**
     * @return RetryHandlerInterface|null
     */
    public function getRetryHandler(): ?RetryHandlerInterface
    {
        return $this->retryHandler;
    }

    /**
     * Returns a value indicating whether the DB connection is established.
     * @return bool whether the DB connection is established
     */
    public function getIsActive(): bool
    {
        return DbContext::get($this->poolName, $this->driver) !== null;
    }

    /**
     * Returns the current query cache information.
     * This method is used internally by [[Command]].
     * @param float|null $duration the preferred caching duration. If null, it will be ignored.
     * @param CacheInterface|null $cache
     * @return array the current query cache information, or null if query cache is not enabled.
     * @throws Throwable
     * @internal
     */
    public function getQueryCacheInfo(?float $duration, ?CacheInterface $cache = null)
    {
        if (!$this->enableQueryCache || $duration === null) {
            return null;
        }

        if ((int)$duration === 0 || $duration > 0) {
            if ($cache === null) {
                if ($this->queryCache === null) {
                    $cache = getDI('cache', false);
                } else {
                    $cache = $this->queryCache;
                }
            }
            if ($cache instanceof CacheInterface) {
                return [$cache, $duration];
            }
        }

        return null;
    }

    /**
     * Closes the currently active DB connection.
     * It does nothing if the connection is already closed.
     * @throws Throwable
     */
    public function close(): void
    {
        $pdo = DbContext::get($this->poolName, $this->driver);
        if ($this->master) {
            if ($pdo === $this->master->getConn()) {
                DbContext::delete($this->poolName, $this->driver);
            }

            $this->master->close();
            $this->master = null;
        }

        if ($pdo !== null) {
            App::warning('Closing DB connection: ' . $this->shortDsn, "db");
            DbContext::delete($this->poolName, $this->driver);
            $this->schema = null;
        }

        if ($this->slave) {
            $this->slave->close();
            $this->slave = null;
        }
    }

    /**
     * Creates a command for execution.
     * @param string|null $sql the SQL statement to be executed
     * @param array $params the parameters to be bound to the SQL statement
     * @return Command the DB command
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Throwable
     * @throws ReflectionException
     */
    public function createCommand(?string $sql = null, array $params = []): Command
    {
        $config = ['class' => $this->commandClass, 'retryHandler' => $this->retryHandler];
        $config['db'] = $this;
        $config['sql'] = $sql;
        /** @var Command $command */
        $command = create($config, [], false);
        return $command->bindValues($params);
    }

    /**
     * Returns the name of the DB driver. Based on the the current [[dsn]], in case it was not set explicitly
     * by an end user.
     * @return string name of the DB driver
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function getDriverName(): string
    {
        if ($this->_driverName === null) {
            if (($pos = strpos($this->dsn, ':')) !== false) {
                $this->_driverName = strtolower(substr($this->dsn, 0, $pos));
            } else {
                $this->_driverName = strtolower($this->getSlavePdo()->getAttribute(PDO::ATTR_DRIVER_NAME));
            }
        }

        return $this->_driverName;
    }

    /**
     * Changes the current driver name.
     * @param string $driverName name of the DB driver
     */
    public function setDriverName(string $driverName)
    {
        $this->_driverName = strtolower($driverName);
    }

    /**
     * Returns the PDO instance for the currently active slave connection.
     * When [[enableSlaves]] is true, one of the slaves will be used for read queries, and its PDO instance
     * will be returned by this method.
     * @param bool $fallbackToMaster whether to return a master PDO in case none of the slave connections is available.
     * @return PDO the PDO instance for the currently active slave connection. `null` is returned if no slave connection
     * is available and `$fallbackToMaster` is false.
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function getSlavePdo(bool $fallbackToMaster = true)
    {
        $db = $this->getSlave(false);
        if ($db === null) {
            return $fallbackToMaster ? $this->getMasterPdo() : null;
        }

        return $db->getConn();
    }

    /**
     * Returns the currently active slave connection.
     * If this method is called for the first time, it will try to open a slave connection when [[enableSlaves]] is true.
     * @param bool $fallbackToMaster whether to return a master connection in case there is no slave connection available.
     * @return Connection the currently active slave connection. `null` is returned if there is no slave available and
     * `$fallbackToMaster` is false.
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function getSlave(bool $fallbackToMaster = true): ?self
    {
        if (!$this->enableSlaves) {
            return $fallbackToMaster ? $this : null;
        }

        if ($this->slave === false) {
            $this->slave = $this->openFromPool($this->slaves, $this->slaveConfig);
        }

        return $this->slave === null && $fallbackToMaster ? $this : $this->slave;
    }

    /**
     * Opens the connection to a server in the pool.
     * This method implements the load balancing among the given list of the servers.
     * Connections will be tried in random order.
     * @param array $pool the list of connection configurations in the server pool
     * @param array $sharedConfig the configuration common to those given in `$pool`.
     * @return Connection the opened DB connection, or `null` if no server is available
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    protected function openFromPool(array $pool, array $sharedConfig): ?self
    {
        shuffle($pool);
        return $this->openFromPoolSequentially($pool, $sharedConfig);
    }

    /**
     * Opens the connection to a server in the pool.
     * This method implements the load balancing among the given list of the servers.
     * Connections will be tried in sequential order.
     * @param array $pool the list of connection configurations in the server pool
     * @param array $sharedConfig the configuration common to those given in `$pool`.
     * @return Connection the opened DB connection, or `null` if no server is available
     * @throws Throwable
     * @throws InvalidArgumentException
     * @since 2.0.11
     */
    protected function openFromPoolSequentially(array $pool, array $sharedConfig): ?self
    {
        if (empty($pool)) {
            return null;
        }

        if (!isset($sharedConfig['class'])) {
            $sharedConfig['class'] = get_class($this);
        }

        $cache = $this->serverStatusCache;

        foreach ($pool as $config) {
            $config = array_merge($sharedConfig, $config);
            if (empty($config['dsn'])) {
                throw new \InvalidArgumentException('The "dsn" option must be specified.');
            }

            $key = [__METHOD__, $config['dsn']];
            if ($cache instanceof CacheInterface && $cache->get($key)) {
                // should not try this dead server now
                continue;
            }

            /* @var $db Connection */
            $db = create($config, []);

            try {
                $db->open();
                return $db;
            } catch (\Exception $e) {
                App::warning("Connection ({$config['dsn']}) failed: " . $e->getMessage(), 'db');
                if ($cache instanceof CacheInterface) {
                    // mark this server as dead and only retry it after the specified interval
                    $cache->set($key, 1, $this->serverRetryInterval);
                }
            }
        }

        return null;
    }

    /**
     * @param int $attempt
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function open(int $attempt = 0)
    {
        if (DbContext::has($this->poolName, $this->driver) === true) {
            return;
        }

        if (!empty($this->masters)) {
            $db = $this->getMaster();
            if ($db !== null) {
                DbContext::set($this->poolName, $db, $this->driver);
                return;
            }

            throw new \InvalidArgumentException('None of the master DB servers is available.');
        }

        if (empty($this->dsn)) {
            throw new \InvalidArgumentException('Connection::dsn cannot be empty.');
        }

        $pdo = $this->getPool()->get();
        if (!$pdo instanceof ConnectionInterface) {
            DbContext::set($this->poolName, $pdo, $this->driver);
        } else {
            $attempt === 0 && ($token = 'Opening DB connection: ' . $this->shortDsn) && App::info($token, "db");
        }
    }

    /**
     * Returns the currently active master connection.
     * If this method is called for the first time, it will try to open a master connection.
     * @return Connection the currently active master connection. `null` is returned if there is no master available.
     * @throws InvalidArgumentException
     * @throws Throwable
     * @since 2.0.11
     */
    public function getMaster(): ?self
    {
        if ($this->master === null) {
            $this->master = $this->shuffleMasters
                ? $this->openFromPool($this->masters, $this->masterConfig)
                : $this->openFromPoolSequentially($this->masters, $this->masterConfig);
        }

        return $this->master;
    }

    /**
     * Creates the PDO instance.
     * This method is called by [[open]] to establish a DB connection.
     * The default implementation will create a PHP PDO instance.
     * You may override this method if the default PDO needs to be adapted for certain DBMS.
     * @return PDO the pdo instance
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function createPdoInstance()
    {
        $pdoClass = $this->pdoClass;
        if ($pdoClass === null) {
            $pdoClass = 'PDO';
        }

        $parsed = $this->parseDsn;
        isset($parsed['query']) ? parse_str($parsed['query'], $parsed['query']) : $parsed['query'] = [];
        [$driver, $host, $port, $this->username, $this->password, $query] = ArrayHelper::getValueByArray(
            $parsed,
            ['scheme', 'host', 'port', 'user', 'pass', 'query'],
            null,
            ['mysql', 'localhost', '3306', '', '', []]
        );
        $parts = [];
        foreach ($query as $key => $value) {
            $parts[] = "$key=$value";
        }
        $dsn = "$driver:host=$host;port=$port;" . implode(';', $parts);
        $pdo = new $pdoClass($dsn, $this->username, $this->password, $this->attributes);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        if ($this->emulatePrepare !== null && constant('PDO::ATTR_EMULATE_PREPARES')) {
            $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES, $this->emulatePrepare);
        }
        if ($this->charset !== null && in_array($this->getDriverName(), ['pgsql', 'mysql', 'mysqli', 'cubrid'], true)) {
            $pdo->exec('SET NAMES ' . $pdo->quote($this->charset));
        }
        return $pdo;
    }

    /**
     * Returns the PDO instance for the currently active master connection.
     * This method will open the master DB connection and then return [[pdo]].
     * @return PDO the PDO instance for the currently active master connection.
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function getMasterPdo()
    {
        $this->open();
        return DbContext::get($this->poolName, $this->driver);
    }

    /**
     * Executes callback provided in a transaction.
     *
     * @param callable $callback a valid PHP callback that performs the job. Accepts connection instance as parameter.
     * @param string|null $isolationLevel The isolation level to use for this transaction.
     * See [[Transaction::begin()]] for details.
     * @return mixed result of callback function
     * @throws Throwable|InvalidArgumentException if there is any exception during query. In this case the transaction will be rolled back.
     */
    public function transaction(callable $callback, string $isolationLevel = null)
    {
        $transaction = $this->beginTransaction($isolationLevel);
        $level = $transaction->level;

        try {
            $result = call_user_func($callback, $this);
            if ($transaction->isActive && $transaction->level === $level) {
                $transaction->commit();
            }
        } catch (Throwable $e) {
            $this->rollbackTransactionOnLevel($transaction, $level);
            throw $e;
        }

        return $result;
    }

    /**
     * @param string|null $isolationLevel
     * @return mixed|Transaction|null
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     */
    public function beginTransaction(string $isolationLevel = null): ?Transaction
    {
        $this->open();

        if (($transaction = $this->getTransaction()) === null) {
            $transaction = new $this->transactionClass($this);
            Context::set('db.transaction', $transaction, $this->poolName);
        }
        $transaction->begin($isolationLevel);

        return $transaction;
    }

    /**
     * Returns the currently active transaction.
     * @return Transaction|null the currently active transaction. Null if no active transaction.
     */
    public function getTransaction(): ?Transaction
    {
        $transaction = Context::get('db.transaction', $this->poolName);
        return $transaction && $transaction->getIsActive() ? $transaction : null;
    }

    /**
     * Rolls back given [[Transaction]] object if it's still active and level match.
     * In some cases rollback can fail, so this method is fail safe. Exception thrown
     * from rollback will be caught and just logged with [[\Yii::error()]].
     * @param Transaction $transaction Transaction object given from [[beginTransaction()]].
     * @param int $level Transaction level just after [[beginTransaction()]] call.
     * @throws Throwable|InvalidArgumentException
     */
    private function rollbackTransactionOnLevel($transaction, $level): void
    {
        if ($transaction->isActive && $transaction->level === $level) {
            // https://github.com/yiisoft/yii2/pull/13347
            try {
                $transaction->rollBack();
            } catch (\Exception $e) {
                App::error($e, "db");
                // hide this exception to be able to continue throwing original exception outside
            }
        }
    }

    /**
     * Can be used to set [[QueryBuilder]] configuration via Connection configuration array.
     *
     * @param iterable $config the [[QueryBuilder]] properties to be configured.
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     * @since 2.0.14
     */
    public function setQueryBuilder(iterable $config): void
    {
        $builder = $this->getQueryBuilder();
        foreach ($config as $key => $value) {
            $builder->{$key} = $value;
        }
    }

    /**
     * Returns the query builder for the current DB connection.
     * @return QueryBuilder the query builder for the current DB connection.
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     */
    public function getQueryBuilder(): QueryBuilder
    {
        return $this->getSchema()->getQueryBuilder();
    }

    /**
     * Returns the schema information for the database opened by this connection.
     * @return Schema the schema information for the database opened by this connection.
     * @throws InvalidArgumentException
     * @throws NotSupportedException if there is no support for the current driver type
     * @throws Throwable
     */
    public function getSchema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        $driver = $this->getDriverName();
        if (isset($this->schemaMap[$driver])) {
            $class = $this->schemaMap[$driver];

            return $this->schema = new $class($this);
        }

        throw new NotSupportedException("Connection does not support reading schema information for '$driver' DBMS.");
    }

    /**
     * Obtains the schema information for the named table.
     * @param string $name table name.
     * @param bool $refresh whether to reload the table schema even if it is found in the cache.
     * @return TableSchema table schema information. Null if the named table does not exist.
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     */
    public function getTableSchema(string $name, bool $refresh = false): ?TableSchema
    {
        return $this->getSchema()->getTableSchema($name, $refresh);
    }

    /**
     * Returns the ID of the last inserted row or sequence value.
     * @param string $sequenceName name of the sequence object (required by some DBMS)
     * @return string the row ID of the last row inserted, or the last value retrieved from the sequence object
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     * @see http://php.net/manual/en/pdo.lastinsertid.php
     */
    public function getLastInsertID(string $sequenceName = '')
    {
        return $this->getSchema()->getLastInsertID($sequenceName);
    }

    /**
     * Quotes a string value for use in a query.
     * Note that if the parameter is not a string, it will be returned without change.
     * @param string $value string to be quoted
     * @return string the properly quoted string
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     * @see http://php.net/manual/en/pdo.quote.php
     */
    public function quoteValue(string $value): string
    {
        return $this->getSchema()->quoteValue($value);
    }

    /**
     * @param string $sql
     * @return string
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     */
    public function quoteSql(string $sql): string
    {
        return preg_replace_callback(
            '/(\\{\\{(%?[\w\-\. ]+%?)\\}\\}|\\[\\[([\w\-\. ]+)\\]\\])/',
            function ($matches) {
                if (isset($matches[3])) {
                    return $this->quoteColumnName($matches[3]);
                }

                return str_replace('%', $this->tablePrefix, $this->quoteTableName($matches[2]));
            },
            $sql
        );
    }

    /**
     * Quotes a column name for use in a query.
     * If the column name contains prefix, the prefix will also be properly quoted.
     * If the column name is already quoted or contains special characters including '(', '[[' and '{{',
     * then this method will do nothing.
     * @param string $name column name
     * @return string the properly quoted column name
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     */
    public function quoteColumnName(string $name): string
    {
        return $this->getSchema()->quoteColumnName($name);
    }

    /**
     * Quotes a table name for use in a query.
     * If the table name contains schema prefix, the prefix will also be properly quoted.
     * If the table name is already quoted or contains special characters including '(', '[[' and '{{',
     * then this method will do nothing.
     * @param string $name table name
     * @return string the properly quoted table name
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     */
    public function quoteTableName(string $name): string
    {
        return $this->getSchema()->quoteTableName($name);
    }

    /**
     * Returns a server version as a string comparable by [[\version_compare()]].
     * @return string server version as a string.
     * @throws InvalidArgumentException
     * @throws NotSupportedException
     * @throws Throwable
     * @since 2.0.14
     */
    public function getServerVersion(): string
    {
        return $this->getSchema()->getServerVersion();
    }

    /**
     * Executes the provided callback by using the master connection.
     *
     * This method is provided so that you can temporarily force using the master connection to perform
     * DB operations even if they are read queries. For example,
     *
     * ```php
     * $result = $db->useMaster(function ($db) {
     *     return $db->createCommand('SELECT * FROM user LIMIT 1')->queryOne();
     * });
     * ```
     *
     * @param callable $callback a PHP callable to be executed by this method. Its signature is
     * `function (Connection $db)`. Its return value will be returned by this method.
     * @return mixed the return value of the callback
     * @throws Throwable if there is any exception thrown from the callback
     */
    public function useMaster(callable $callback)
    {
        if ($this->enableSlaves) {
            $this->enableSlaves = false;
            try {
                $result = call_user_func($callback, $this);
            } catch (Throwable $e) {
                $this->enableSlaves = true;
                throw $e;
            }
            // TODO: use "finally" keyword when miminum required PHP version is >= 5.5
            $this->enableSlaves = true;
        } else {
            $result = call_user_func($callback, $this);
        }

        return $result;
    }

    /**
     * Close the connection before serializing.
     * @return array
     */
    public function __sleep()
    {
        $fields = (array)$this;

        unset($fields['pdo']);
        unset($fields["\000" . __CLASS__ . "\000" . '_master']);
        unset($fields["\000" . __CLASS__ . "\000" . '_slave']);
        unset($fields["\000" . __CLASS__ . "\000" . '_transaction']);
        unset($fields["\000" . __CLASS__ . "\000" . '_schema']);

        return array_keys($fields);
    }

    /**
     * Reset the connection after cloning.
     */
    public function __clone()
    {
        $this->master = null;
        $this->slave = null;
        $this->schema = null;
        if (strncmp($this->dsn, 'sqlite::memory:', 15) !== 0) {
            // reset PDO connection, unless its sqlite in-memory, which can only have one connection
            DbContext::delete($this->poolName, $this->driver);
        }
    }

    protected function makeShortDsn(): void
    {
        $parsed = parse_url($this->dsn);
        $this->parseDsn = is_array($parsed) ? $parsed : [];
        if (!isset($parsed['path'])) {
            $parsed['path'] = '/';
        }
        $this->shortDsn = UrlHelper::unParseUrl($parsed, false);
    }
}
