<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use PDO;
use Psr\SimpleCache\CacheInterface;
use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\UrlHelper;
use Rabbit\Cache\ArrayCache;
use Rabbit\Pool\PoolManager;
use Throwable;

class Connection extends BaseObject implements ConnectionInterface
{
    use ConnectionTrait;

    const SHARE_ARRAY = 'share';
    const SHARE_PROCESS = 'process_share';

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
    public int $share = 3;
    public string $shareCache = 'share';
    public bool $canTransaction = true;
    protected string $transactionClass = Transaction::class;
    protected ?Schema $schema = null;
    protected ?string $_driverName = null;
    protected ?Connection $master = null;
    protected ?Connection $slave = null;
    protected ?RetryHandlerInterface $retryHandler = null;
    protected string $commandClass = Command::class;
    protected string $driver = 'pdo';
    protected ?array $parseDsn = [];

    /**
     * Connection constructor.
     * @param string $dsn
     */
    public function __construct(protected string $dsn)
    {
        $this->schemaCache = new ArrayCache();
        $this->serverStatusCache = new ArrayCache();
        $this->makeShortDsn();
    }

    public function getDriver(): string
    {
        return $this->driver;
    }

    public function getConn(): object
    {
        return DbContext::get($this->poolKey);
    }

    public function getRetryHandler(): ?RetryHandlerInterface
    {
        return $this->retryHandler;
    }

    public function getIsActive(): bool
    {
        return true;
    }

    public function getQueryCacheInfo(?float $duration, ?CacheInterface $cache = null): ?array
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

    public function close(): void
    {
        $pdo = DbContext::get($this->poolKey);
        if ($this->master) {
            if ($pdo === $this->master->getConn()) {
                DbContext::delete($this->poolKey);
            }

            $this->master->close();
            $this->master = null;
        }

        if ($pdo !== null) {
            App::warning('Closing DB connection: ' . $this->shortDsn, "db");
            DbContext::delete($this->poolKey);
            $this->schema = null;
        }

        if ($this->slave) {
            $this->slave->close();
            $this->slave = null;
        }
    }

    public function createCommand(?string $sql = null, array $params = []): Command
    {
        $config = ['class' => $this->commandClass, 'retryHandler' => $this->retryHandler];
        $config['db'] = $this;
        $config['sql'] = $sql;
        /** @var Command $command */
        $command = create($config, [], false);
        return $command->bindValues($params);
    }

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

    public function setDriverName(string $driverName): void
    {
        $this->_driverName = strtolower($driverName);
    }

    public function getSlavePdo(bool $fallbackToMaster = true): object
    {
        $db = $this->getSlave(false);
        if ($db === null) {
            return $fallbackToMaster ? $this->getMasterPdo() : null;
        }

        return $db->getConn();
    }

    public function getSlave(bool $fallbackToMaster = true): ?self
    {
        if (!$this->enableSlaves) {
            return $fallbackToMaster ? $this : null;
        }

        if ($this->slave === null) {
            $this->slave = $this->openFromPool($this->slaves, $this->slaveConfig);
        }

        return $this->slave === null && $fallbackToMaster ? $this : $this->slave;
    }

    protected function openFromPool(array $pool, array $sharedConfig): ?self
    {
        shuffle($pool);
        return $this->openFromPoolSequentially($pool, $sharedConfig);
    }

    protected function openFromPoolSequentially(array $pool, array $sharedConfig): ?self
    {
        if (empty($pool)) {
            return null;
        }

        if (!isset($sharedConfig['class'])) {
            $sharedConfig['class'] = get_class($this);
        }

        $cache = $this->serverStatusCache;

        if (PoolManager::getPool($this->poolKey . '.slave') === null) {
            $slavePool = clone $this->getPool();
            $slavePool->getPoolConfig()->setName($this->poolKey . '.slave');
            PoolManager::setPool($slavePool);
        }
        foreach ($pool as $config) {
            $config = [...$sharedConfig, ...$config];
            if (empty($config['dsn'])) {
                throw new \InvalidArgumentException('The "dsn" option must be specified.');
            }

            $key = [__METHOD__, $config['dsn']];
            if ($cache instanceof CacheInterface && $cache->get($key)) {
                // should not try this dead server now
                continue;
            }

            /* @var $db Connection */
            $db = create($config, ['poolKey' => $this->poolKey . '.slave']);
            $slaveConfig = PoolManager::getPool($this->poolKey . '.slave')->getPoolConfig();
            $slaveConfig->setConfig([...$slaveConfig->getConfig(), 'conn' => $db]);
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

    public function open(int $attempt = 0): void
    {
        if (DbContext::has($this->poolKey) === true) {
            return;
        }

        if (!empty($this->masters)) {
            $db = $this->getMaster();
            if ($db !== null) {
                DbContext::set($this->poolKey, $db);
                return;
            }

            throw new \InvalidArgumentException('None of the master DB servers is available.');
        }

        if (empty($this->dsn)) {
            throw new \InvalidArgumentException('Connection::dsn cannot be empty.');
        }

        $pdo = $this->getPool()->get();
        if (!$pdo instanceof ConnectionInterface) {
            DbContext::set($this->poolKey, $pdo);
        } else {
            DbContext::set($this->poolKey, $pdo->createPdoInstance());
        }
    }

    public function getMaster(): ?self
    {
        if ($this->master === null) {
            $this->master = $this->shuffleMasters
                ? $this->openFromPool($this->masters, $this->masterConfig)
                : $this->openFromPoolSequentially($this->masters, $this->masterConfig);
        }

        return $this->master;
    }

    public function createPdoInstance(): object
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
        ($token = 'Opening DB connection: ' . $this->shortDsn) && App::info($token, "db");
        return $pdo;
    }

    public function getMasterPdo(): object
    {
        $this->open();
        return DbContext::get($this->poolKey);
    }

    public function transaction(callable $callback, null|string $isolationLevel = null)
    {
        $transaction = $this->beginTransaction($isolationLevel);
        $level = $transaction->level;

        try {
            $result = call_user_func($callback, $this);
            if ($transaction->getIsActive() && $transaction->level === $level) {
                $transaction->commit();
            }
        } catch (Throwable $e) {
            $this->rollbackTransactionOnLevel($transaction, $level);
            throw $e;
        }

        return $result;
    }

    public function beginTransaction(null|string $isolationLevel = null): ?Transaction
    {
        $this->open();

        if (null === $transaction = $this->getTransaction()) {
            $transaction = new $this->transactionClass($this);
            Context::set('db.transaction', $transaction, $this->poolKey);
        }
        $transaction->begin($isolationLevel);

        return $transaction;
    }

    public function getTransaction(): ?Transaction
    {
        return Context::get('db.transaction', $this->poolKey);
    }

    private function rollbackTransactionOnLevel(Transaction $transaction, int $level): void
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

    public function setQueryBuilder(iterable $config): void
    {
        $builder = $this->getQueryBuilder();
        foreach ($config as $key => $value) {
            $builder->{$key} = $value;
        }
    }

    public function getQueryBuilder(): QueryBuilder
    {
        return $this->getSchema()->getQueryBuilder();
    }

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

    public function getTableSchema(string $name, bool $refresh = false): ?TableSchema
    {
        return $this->getSchema()->getTableSchema($name, $refresh);
    }

    public function getLastInsertID(string $sequenceName = ''): string
    {
        return $this->getSchema()->getLastInsertID($sequenceName);
    }

    public function quoteValue(string $value): string
    {
        return $this->getSchema()->quoteValue($value);
    }

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

    public function quoteColumnName(string $name): string
    {
        return $this->getSchema()->quoteColumnName($name);
    }

    public function quoteTableName(string $name): string
    {
        return $this->getSchema()->quoteTableName($name);
    }

    public function getServerVersion(): string
    {
        return $this->getSchema()->getServerVersion();
    }

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

    public function __clone()
    {
        $this->master = null;
        $this->slave = null;
        $this->schema = null;
        if (strncmp($this->dsn, 'sqlite::memory:', 15) !== 0) {
            // reset PDO connection, unless its sqlite in-memory, which can only have one connection
            DbContext::delete($this->poolKey);
        }
    }

    protected function makeShortDsn(): void
    {
        $parsed = parse_url($this->dsn);
        $this->parseDsn = is_array($parsed) ? UrlHelper::unParseUrlArray($parsed) : [];
        if (!isset($parsed['path'])) {
            $parsed['path'] = '/';
        }
        $this->shortDsn = UrlHelper::unParseUrl($parsed, false);
    }

    public function buildQuery(): QueryInterface
    {
        return new Query($this);
    }
}
