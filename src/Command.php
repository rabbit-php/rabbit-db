<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Generator;
use Throwable;
use PDOException;
use PDOStatement;
use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Psr\SimpleCache\CacheInterface;
use Rabbit\Server\ProcessShare;

class Command extends BaseObject
{
    public ?ConnectionInterface $db = null;
    public ?PDOStatement $pdoStatement = null;
    public int $fetchMode = \PDO::FETCH_ASSOC;
    public array $params = [];
    public ?float $queryCacheDuration = null;
    protected array $_pendingParams = [];
    protected ?string $sql = null;
    protected ?string $_refreshTableName = null;
    protected ?bool $_isolationLevel = false;
    protected ?CacheInterface $cache = null;
    protected ?int $share = null;
    protected string $shareType = Connection::SHARE_PROCESS;

    public function __destruct()
    {
        $this->pdoStatement = null;
        $this->db && $this->db->release();
    }

    public function cache(float $duration = 0, ?CacheInterface $cache = null): self
    {
        $this->queryCacheDuration = $duration;
        $this->cache = $cache;
        return $this;
    }

    public function noCache(): self
    {
        $this->queryCacheDuration = null;
        return $this;
    }

    public function share(int $timeout = null): self
    {
        $this->share = $timeout;
        return $this;
    }

    public function shareType(string $type): self
    {
        $this->shareType = $type;
        return $this;
    }

    public function bindParam(string $name, string|float|int|array|null &$value, ?int $dataType = null, ?int $length = null, $driverOptions = null): self
    {
        $this->prepare();

        if ($dataType === null) {
            $dataType = $this->db->getSchema()->getPdoType($value);
        }
        if ($length === null) {
            $this->pdoStatement->bindParam($name, $value, $dataType);
        } elseif ($driverOptions === null) {
            $this->pdoStatement->bindParam($name, $value, $dataType, $length);
        } else {
            $this->pdoStatement->bindParam($name, $value, $dataType, $length, $driverOptions);
        }
        $this->params[$name] = &$value;

        return $this;
    }

    public function prepare(bool $forRead = null): void
    {
        if ($this->pdoStatement) {
            $this->bindPendingParams();
            return;
        }

        $sql = $this->sql;

        if ($this->db->getTransaction()) {
            // master is in a transaction. use the same connection.
            $forRead = false;
        }

        if ($forRead || $forRead === null && $this->db->getSchema()->isReadQuery($sql)) {
            $pdo = $this->db->getSlavePdo();
        } else {
            $pdo = $this->db->getMasterPdo();
        }
        if ($pdo === null) {
            throw new Exception('Can not get the connection!');
        }
        try {
            $this->pdoStatement = $pdo->prepare($sql);
            $this->bindPendingParams();
        } catch (Throwable $e) {
            $message = $e->getMessage() . " Failed to prepare SQL: $sql";
            $errorInfo = $e instanceof PDOException ? $e->errorInfo : null;
            $e = new Exception($message, $errorInfo, (int)$e->getCode(), $e);
            throw $e;
        }
    }

    protected function bindPendingParams(): void
    {
        foreach ($this->_pendingParams as $name => $value) {
            $this->pdoStatement->bindValue(is_int($name) ? $name + 1 : $name, $value[0], $value[1]);
        }
    }

    public function getSql(): ?string
    {
        return $this->sql;
    }

    public function setSql(?string $sql): self
    {
        if ($sql !== $this->sql) {
            $this->cancel();
            $this->reset();
            $this->sql = $this->db->quoteSql($sql);
        }

        return $this;
    }

    public function bindValue(string $name, string|float|int|array|null &$value, int $dataType = null): self
    {
        if ($dataType === null) {
            $dataType = $this->db->getSchema()->getPdoType($value);
        }
        $this->_pendingParams[$name] = [&$value, $dataType];
        $this->params[$name] = &$value;

        return $this;
    }

    public function query(): DataReader
    {
        return $this->queryInternal('');
    }

    protected function queryInternal(string $method, int $fetchMode = null): null|string|bool|int|float|array|DataReader
    {
        $rawSql = $this->getRawSql();
        $share = $this->share ?? $this->db->share;
        $func = function () use ($method, &$rawSql, $fetchMode): mixed {
            if ($method !== '') {
                $info = $this->db->getQueryCacheInfo($this->queryCacheDuration, $this->cache);
                if (is_array($info)) {
                    /* @var $cache CacheInterface */
                    $cache = $info[0];
                    $cacheKey = array_filter([
                        __CLASS__,
                        $method,
                        $fetchMode,
                        $this->db->dsn,
                        $rawSql,
                    ]);
                    $cacheKey = extension_loaded('msgpack') ? \msgpack_pack($cacheKey) : serialize($cacheKey);
                    $cacheKey = md5($cacheKey);
                    if (!empty($ret = $cache->get($cacheKey))) {
                        $result = unserialize($ret);
                        if (is_array($result) && isset($result[0])) {
                            $rawSql .= '; [Query result read from cache]';
                            $this->logQuery($rawSql);
                            return $result[0];
                        }
                    }
                }
            }

            $this->logQuery($rawSql);

            try {
                $this->internalExecute($rawSql);
                if ($method === '') {
                    $result = new DataReader($this);
                } else {
                    if ($fetchMode === null) {
                        $fetchMode = $this->fetchMode;
                    }
                    $result = call_user_func_array([$this->pdoStatement, $method], (array)$fetchMode);
                    $this->pdoStatement->closeCursor();
                }
            } catch (Throwable $e) {
                throw $e;
            }

            if (isset($cache, $cacheKey, $info)) {
                $log = 'Saved query result in cache';
                !$cache->has($cacheKey) && $cache->set($cacheKey, serialize([$result]), $info[1]) && $this->logQuery($log);
            }

            return $result;
        };
        if ($share > 0) {
            $cacheKey = array_filter([
                __CLASS__,
                $method,
                $fetchMode,
                $this->db->dsn,
                $rawSql,
            ]);
            $cacheKey = extension_loaded('msgpack') ? \msgpack_pack($cacheKey) : serialize($cacheKey);
            $cacheKey = md5($cacheKey);
            $type = $this->shareType;
            $s = $type($cacheKey, $func, $share, $this->db->shareCache);
            $status = $s->getStatus();
            if ($status === SWOOLE_CHANNEL_CLOSED) {
                $rawSql .= '; [Query result read from channel share]';
                $this->logQuery($rawSql);
            } elseif ($status === ProcessShare::STATUS_PROCESS) {
                $rawSql .= '; [Query result read from process share]';
                $this->logQuery($rawSql);
            } elseif ($status === ProcessShare::STATUS_CHANNEL) {
                $rawSql .= '; [Query result read from process channel share]';
                $this->logQuery($rawSql);
            }
            return $s->result;
        }
        return $func();
    }

    protected function logQuery(string &$rawSql): void
    {
        if ($this->db->enableLogging && ($this->db->maxLog === 0 || ($this->db->maxLog > 0 && strlen($rawSql) < $this->db->maxLog))) {
            App::info($this->db->shortDsn . PHP_EOL . $rawSql);
        }
    }

    public function getRawSql(): string
    {
        if (empty($this->params)) {
            return $this->sql;
        }
        $sql = '';
        $sqlArr = explode('?', $this->sql);
        foreach ($this->params as $i => $value) {
            $sql .= $sqlArr[$i];
            if (is_string($value)) {
                $sql .= $this->db->quoteValue($value);
            } elseif (is_bool($value)) {
                $sql .=  ($value ? 'TRUE' : 'FALSE');
            } elseif ($value === null) {
                $sql .=  'NULL';
            } elseif ((!is_object($value) && !is_resource($value)) || $value instanceof Expression) {
                $sql .= $value;
            }
        }
        if ($i !== count($sqlArr) - 1) {
            $sql .= end($sqlArr);
        }
        return $sql;
    }

    protected function internalExecute(string &$rawSql): void
    {
        $attempt = 0;
        while (true) {
            try {
                $attempt++;
                $this->prepare(true);
                $this->pdoStatement->execute();
                if ($this->pdoStatement->errorCode() !== '00000') {
                    $errArr = $this->pdoStatement->errorInfo();
                    throw new Exception(end($errArr), $this->pdoStatement->errorInfo());
                }
                $this->_pendingParams = [];
                return;
            } catch (\Exception $e) {
                $this->pdoStatement && $this->pdoStatement->closeCursor();
                $e = $this->db->getSchema()->convertException($e, $rawSql);
                if (($retryHandler = $this->db->getRetryHandler()) === null || (RetryHandlerInterface::RETRY_NO === $code = $retryHandler->handle($e, $attempt))) {
                    $this->pdoStatement = null;
                    throw $e;
                }
                if ($code === RetryHandlerInterface::RETRY_CONNECT) {
                    $this->pdoStatement = null;
                    $this->db->reconnect($attempt);
                }
            }
        }
    }

    public function queryAll(int $fetchMode = null): array
    {
        $res = $this->queryInternal('fetchAll', $fetchMode);
        if (!is_array($res)) {
            return [];
        }
        return $res;
    }

    public function queryOne(int $fetchMode = null): ?array
    {
        if (false === $result = $this->queryInternal('fetch', $fetchMode)) {
            return null;
        }
        return $result;
    }

    public function queryScalar(): null|string|bool|int|float|array
    {
        $result = $this->queryInternal('fetchColumn', 0);
        if (is_resource($result) && get_resource_type($result) === 'stream') {
            if (false === $res = stream_get_contents($result)) {
                return null;
            }
            return $res;
        }

        return $result;
    }

    public function queryColumn(): ?array
    {
        return $this->queryInternal('fetchAll', \PDO::FETCH_COLUMN);
    }

    public function insert(string $table, array|Query $columns, bool $withUpdate = false): self
    {
        $params = [];
        $sql = $this->db->getQueryBuilder()->insert($table, $columns, $params, $withUpdate);

        return $this->setSql($sql)->bindValues($params);
    }

    public function bindValues(array $values): self
    {
        if (empty($values)) {
            return $this;
        }

        $schema = $this->db->getSchema();
        foreach ($values as $name => $value) {
            if ($value instanceof PdoValue) {
                $this->_pendingParams[$name] = [$value->getValue(), $value->getType()];
                $this->params[$name] = $value->getValue();
            } else {
                $type = $schema->getPdoType($value);
                $this->_pendingParams[$name] = [$value, $type];
                $this->params[$name] = $value;
            }
        }

        return $this;
    }

    public function batchInsert(string $table, array $columns, array|Generator $rows): self
    {
        $table = $this->db->quoteSql($table);
        $columns = array_map(function ($column): string {
            return $this->db->quoteSql($column);
        }, $columns);

        $params = [];
        $sql = $this->db->getQueryBuilder()->batchInsert($table, $columns, $rows, $params);

        $this->setRawSql($sql);
        $this->bindValues($params);

        return $this;
    }

    public function setRawSql(string $sql): self
    {
        if ($sql !== $this->sql) {
            $this->cancel();
            $this->reset();
            $this->sql = $sql;
        }

        return $this;
    }

    public function cancel(): void
    {
        $this->pdoStatement = null;
    }

    protected function reset(): void
    {
        $this->sql = null;
        $this->_pendingParams = [];
        $this->params = [];
        $this->_refreshTableName = null;
        $this->_isolationLevel = false;
    }

    public function upsert(string $table, array|Query $insertColumns, array|bool $updateColumns = true, array $params = []): self
    {
        $sql = $this->db->getQueryBuilder()->upsert($table, $insertColumns, $updateColumns, $params);

        return $this->setSql($sql)->bindValues($params);
    }

    public function update(string $table, array $columns, string|array $condition = '', array $params = []): self
    {
        $sql = $this->db->getQueryBuilder()->update($table, $columns, $condition, $params);

        return $this->setSql($sql)->bindValues($params);
    }

    public function delete(string $table, string|array $condition = '', array $params = []): self
    {
        $sql = $this->db->getQueryBuilder()->delete($table, $condition, $params);

        return $this->setSql($sql)->bindValues($params);
    }

    public function createTable(string $table, array $columns, string $options = null): self
    {
        $sql = $this->db->getQueryBuilder()->createTable($table, $columns, $options);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    protected function requireTableSchemaRefresh(string $name): self
    {
        $this->_refreshTableName = $name;
        return $this;
    }

    public function renameTable(string $table, string $newName): self
    {
        $sql = $this->db->getQueryBuilder()->renameTable($table, $newName);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function dropTable(string $table): self
    {
        $sql = $this->db->getQueryBuilder()->dropTable($table);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function truncateTable(string $table): self
    {
        $sql = $this->db->getQueryBuilder()->truncateTable($table);

        return $this->setSql($sql);
    }

    public function addColumn(string $table, string $column, string $type): self
    {
        $sql = $this->db->getQueryBuilder()->addColumn($table, $column, $type);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function dropColumn(string $table, string $column): self
    {
        $sql = $this->db->getQueryBuilder()->dropColumn($table, $column);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function renameColumn(string $table, string $oldName, string $newName): self
    {
        $sql = $this->db->getQueryBuilder()->renameColumn($table, $oldName, $newName);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function alterColumn(string $table, string $column, string $type): self
    {
        $sql = $this->db->getQueryBuilder()->alterColumn($table, $column, $type);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function addPrimaryKey(string $name, string $table, string|array $columns): self
    {
        $sql = $this->db->getQueryBuilder()->addPrimaryKey($name, $table, $columns);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function dropPrimaryKey(string $name, string $table): self
    {
        $sql = $this->db->getQueryBuilder()->dropPrimaryKey($name, $table);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function addForeignKey(string $name, string $table, string|array $columns, string $refTable, string|array $refColumns, string $delete = null, string $update = null): self
    {
        $sql = $this->db->getQueryBuilder()->addForeignKey(
            $name,
            $table,
            $columns,
            $refTable,
            $refColumns,
            $delete,
            $update
        );

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function dropForeignKey(string $name, string $table): self
    {
        $sql = $this->db->getQueryBuilder()->dropForeignKey($name, $table);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function createIndex(string $name, string $table, string|array $columns, bool $unique = false): self
    {
        $sql = $this->db->getQueryBuilder()->createIndex($name, $table, $columns, $unique);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function dropIndex(string $name, string $table): self
    {
        $sql = $this->db->getQueryBuilder()->dropIndex($name, $table);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function addUnique(string $name, string $table, string|array $columns): self
    {
        $sql = $this->db->getQueryBuilder()->addUnique($name, $table, $columns);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function dropUnique(string $name, string $table): self
    {
        $sql = $this->db->getQueryBuilder()->dropUnique($name, $table);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function addCheck(string $name, string $table, string $expression): self
    {
        $sql = $this->db->getQueryBuilder()->addCheck($name, $table, $expression);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function dropCheck(string $name, string $table): self
    {
        $sql = $this->db->getQueryBuilder()->dropCheck($name, $table);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function resetSequence(string $table, array|string $value = null): self
    {
        $sql = $this->db->getQueryBuilder()->resetSequence($table, $value);

        return $this->setSql($sql);
    }

    public function checkIntegrity(string $schema = '', string $table = '', bool $check = true): self
    {
        $sql = $this->db->getQueryBuilder()->checkIntegrity($schema, $table, $check);

        return $this->setSql($sql);
    }

    public function addCommentOnColumn(string $table, string $column, string $comment): self
    {
        $sql = $this->db->getQueryBuilder()->addCommentOnColumn($table, $column, $comment);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function addCommentOnTable(string $table, string $comment): self
    {
        $sql = $this->db->getQueryBuilder()->addCommentOnTable($table, $comment);

        return $this->setSql($sql);
    }

    public function dropCommentFromColumn(string $table, string $column): self
    {
        $sql = $this->db->getQueryBuilder()->dropCommentFromColumn($table, $column);

        return $this->setSql($sql)->requireTableSchemaRefresh($table);
    }

    public function dropCommentFromTable(string $table): self
    {
        $sql = $this->db->getQueryBuilder()->dropCommentFromTable($table);

        return $this->setSql($sql);
    }

    public function createView(string $viewName, string|Query $subquery): self
    {
        $sql = $this->db->getQueryBuilder()->createView($viewName, $subquery);

        return $this->setSql($sql)->requireTableSchemaRefresh($viewName);
    }

    public function dropView(string $viewName): self
    {
        $sql = $this->db->getQueryBuilder()->dropView($viewName);

        return $this->setSql($sql)->requireTableSchemaRefresh($viewName);
    }

    public function execute(): int
    {
        $sql = $this->sql;
        $rawSql = $this->getRawSql();
        $this->logQuery($rawSql);
        if ($sql === '') {
            return 0;
        }

        try {
            $this->internalExecute($rawSql);
            $n = $this->pdoStatement->rowCount();
            $this->db->setInsertId();
            $this->refreshTableSchema();
            return $n;
        } catch (Exception $e) {
            throw $e;
        }
    }

    protected function refreshTableSchema(): void
    {
        if ($this->_refreshTableName !== null) {
            $this->db->getSchema()->refreshTableSchema($this->_refreshTableName);
        }
    }

    protected function requireTransaction(?string $isolationLevel = null): self
    {
        $this->_isolationLevel = $isolationLevel;
        return $this;
    }
}
