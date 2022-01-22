<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Throwable;
use PDOException;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Core\BaseObject;
use Psr\SimpleCache\CacheInterface;
use Rabbit\Base\Exception\InvalidCallException;
use Rabbit\Base\Exception\NotSupportedException;

abstract class Schema extends BaseObject
{
    // The following are the supported abstract column data types.
    const TYPE_PK = 'pk';
    const TYPE_UPK = 'upk';
    const TYPE_BIGPK = 'bigpk';
    const TYPE_UBIGPK = 'ubigpk';
    const TYPE_CHAR = 'char';
    const TYPE_STRING = 'string';
    const TYPE_TEXT = 'text';
    const TYPE_MEDIUMTEXT = 'mediumtext';
    const TYPE_TINYTEXT = 'tinytext';
    const TYPE_LONGTEXT = 'longtext';
    const TYPE_TINYINT = 'tinyint';
    const TYPE_SMALLINT = 'smallint';
    const TYPE_INTEGER = 'integer';
    const TYPE_BIGINT = 'bigint';
    const TYPE_FLOAT = 'float';
    const TYPE_DOUBLE = 'double';
    const TYPE_DECIMAL = 'decimal';
    const TYPE_DATETIME = 'datetime';
    const TYPE_TIMESTAMP = 'timestamp';
    const TYPE_TIME = 'time';
    const TYPE_DATE = 'date';
    const TYPE_BINARY = 'binary';
    const TYPE_BOOLEAN = 'boolean';
    const TYPE_MONEY = 'money';
    const TYPE_JSON = 'json';
    const SCHEMA_CACHE_VERSION = 1;
    protected ?string $defaultSchema;
    protected array $exceptionMap = [
        'SQLSTATE[23' => IntegrityException::class,
    ];
    protected string $columnSchemaClass = ColumnSchema::class;
    protected string $tableQuoteCharacter = "'";
    protected string $columnQuoteCharacter = '"';
    private ?array $_schemaNames = null;
    private array $_tableNames = [];
    private array $_tableMetadata = [];
    private ?QueryBuilder $_builder = null;
    protected string $builderClass = QueryBuilder::class;
    private ?string $_serverVersion = null;

    public function __construct(public readonly Connection $db)
    {
    }

    public function getTableSchemas(string $schema = '', bool $refresh = false): array
    {
        return $this->getSchemaMetadata($schema, 'schema', $refresh);
    }

    protected function getSchemaMetadata(string $schema, string $type, bool $refresh): array
    {
        $metadata = [];
        $methodName = 'getTable' . ucfirst($type);
        foreach ($this->getTableNames($schema, $refresh) as $name) {
            if ($schema !== '') {
                $name = $schema . '.' . $name;
            }
            $tableMetadata = $this->$methodName($name, $refresh);
            if ($tableMetadata !== null) {
                $metadata[] = $tableMetadata;
            }
        }

        return $metadata;
    }

    public function getTableNames(string $schema = '', bool $refresh = false): array
    {
        if (!isset($this->_tableNames[$schema]) || $refresh) {
            $this->_tableNames[$schema] = $this->findTableNames($schema);
        }

        return $this->_tableNames[$schema];
    }

    protected function findTableNames(string $schema = ''): ?array
    {
        throw new NotSupportedException(get_class($this) . ' does not support fetching all table names.');
    }

    public function getSchemaNames(bool $refresh = false): array
    {
        if ($this->_schemaNames === null || $refresh) {
            $this->_schemaNames = $this->findSchemaNames();
        }

        return $this->_schemaNames;
    }

    protected function findSchemaNames(): array
    {
        throw new NotSupportedException(get_class($this) . ' does not support fetching all schema names.');
    }

    public function getQueryBuilder(): QueryBuilder
    {
        if ($this->_builder === null) {
            $this->_builder = $this->createQueryBuilder();
        }

        return $this->_builder;
    }

    public function createQueryBuilder(): QueryBuilder
    {
        return new $this->builderClass($this->db);
    }

    public function getPdoType($data): int
    {
        static $typeMap = [
            // php type => PDO type
            'boolean' => \PDO::PARAM_BOOL,
            'integer' => \PDO::PARAM_INT,
            'string' => \PDO::PARAM_STR,
            'resource' => \PDO::PARAM_LOB,
            'NULL' => \PDO::PARAM_NULL,
        ];
        $type = gettype($data);

        return $typeMap[$type] ?? \PDO::PARAM_STR;
    }

    public function refresh(): void
    {
        $this->_tableNames = [];
        $this->_tableMetadata = [];
    }

    public function refreshTableSchema(string $name): void
    {
        $rawName = $this->getRawTableName($name);
        unset($this->_tableMetadata[$rawName]);
        $this->_tableNames = [];
        $this->db->enableSchemaCache && $this->db->schemaCache->delete($this->getCacheKey($rawName));
    }

    public function getRawTableName(string $name): string
    {
        if (strpos($name, '{{') !== false) {
            $name = preg_replace('/\\{\\{(.*?)\\}\\}/', '\1', $name);

            return str_replace('%', $this->db->tablePrefix, $name);
        }

        return $name;
    }

    protected function getCacheKey(string $name): array
    {
        return [
            __CLASS__,
            $this->db->dsn,
            $this->db->username,
            $this->getRawTableName($name),
        ];
    }

    public function createColumnSchemaBuilder(string $type, int|string|array $length = null): ColumnSchemaBuilder
    {
        return new ColumnSchemaBuilder($type, $length, $this->db);
    }

    public function findUniqueIndexes(TableSchema $table): array
    {
        throw new NotSupportedException(get_class($this) . ' does not support getting unique indexes information.');
    }

    public function supportsSavepoint(): bool
    {
        return $this->db->enableSavepoint;
    }

    public function createSavepoint(string $name): void
    {
        $this->db->createCommand("SAVEPOINT $name")->execute();
    }

    public function releaseSavepoint(string $name): void
    {
        $this->db->createCommand("RELEASE SAVEPOINT $name")->execute();
    }

    public function rollBackSavepoint(string $name): void
    {
        $this->db->createCommand("ROLLBACK TO SAVEPOINT $name")->execute();
    }

    public function setTransactionIsolationLevel(string $level): void
    {
        $this->db->createCommand("SET TRANSACTION ISOLATION LEVEL $level")->execute();
    }

    public function insert(string $table, array $columns): ?array
    {
        $tableSchema = $this->getTableSchema($table);
        $command = $this->db->createCommand()->insert($table, $columns);
        if (!$command->execute()) {
            return null;
        }
        $result = [];
        foreach ($tableSchema->primaryKey as $name) {
            if ($tableSchema->columns[$name]->autoIncrement) {
                $result[$name] = $this->getLastInsertID($tableSchema->sequenceName);
                break;
            }

            $result[$name] = $columns[$name] ?? $tableSchema->columns[$name]->defaultValue;
        }

        return $result;
    }

    public function getTableSchema(string $name, bool $refresh = false): ?TableSchema
    {
        $key = $this->db->getPoolKey() . ':' . $name;
        return share($key, function () use ($name, $refresh): ?TableSchema {
            return $this->getTableMetadata($name, 'schema', $refresh);
        })->result;
    }

    protected function getTableMetadata(string $name, string $type, bool $refresh): ?TableSchema
    {
        $cache = null;
        if ($this->db->enableSchemaCache && !in_array($name, $this->db->schemaCacheExclude, true)) {
            $cache = $this->db->schemaCache;
        }
        $rawName = $this->getRawTableName($name);
        if (!isset($this->_tableMetadata[$rawName])) {
            $this->loadTableMetadataFromCache($cache, $rawName);
        }
        if ($refresh || !array_key_exists($type, $this->_tableMetadata[$rawName])) {
            $this->_tableMetadata[$rawName][$type] = $this->{'loadTable' . ucfirst($type)}($rawName);
            $this->saveTableMetadataToCache($cache, $rawName);
        }

        return $this->_tableMetadata[$rawName][$type];
    }

    protected function setTableMetadata(string $name, string $type, ?TableSchema $data): void
    {
        $this->_tableMetadata[$this->getRawTableName($name)][$type] = $data;
    }

    private function loadTableMetadataFromCache(?CacheInterface $cache, string $name): void
    {
        if ($cache === null) {
            $this->_tableMetadata[$name] = [];
            return;
        }

        $metadata = $cache->get($this->getCacheKey($name));
        if (!is_array($metadata) || !isset($metadata['cacheVersion']) || $metadata['cacheVersion'] !== static::SCHEMA_CACHE_VERSION) {
            $this->_tableMetadata[$name] = [];
            return;
        }

        unset($metadata['cacheVersion']);
        $this->_tableMetadata[$name] = $metadata;
    }

    private function saveTableMetadataToCache(?CacheInterface $cache, string $name): void
    {
        if ($cache === null) {
            return;
        }

        $metadata = $this->_tableMetadata[$name];
        $metadata['cacheVersion'] = static::SCHEMA_CACHE_VERSION;
        $cache->set(
            $this->getCacheKey($name),
            $metadata,
            $this->db->schemaCacheDuration
        );
    }

    public function getLastInsertID(string $sequenceName = ''): null|string|int
    {
        if (null !== $id = Context::get($this->db->getPoolKey() . '.id')) {
            return $id;
        }

        throw new InvalidCallException('DB Connection is not get insert id.');
    }

    public function quoteTableName(string $name): string
    {
        if (strpos($name, '(') !== false || strpos($name, '{{') !== false) {
            return $name;
        }
        if (strpos($name, '.') === false) {
            return $this->quoteSimpleTableName($name);
        }
        $parts = explode('.', $name);
        foreach ($parts as $i => $part) {
            $parts[$i] = $this->quoteSimpleTableName($part);
        }

        return implode('.', $parts);
    }

    public function quoteSimpleTableName(string $name): string
    {
        $startingCharacter = $endingCharacter = $this->tableQuoteCharacter;
        return strpos($name, $startingCharacter) !== false ? $name : $startingCharacter . $name . $endingCharacter;
    }

    public function quoteValue(string $str): string
    {
        if (($value = $this->db->getSlavePdo()->quote($str)) !== false) {
            return $value;
        }

        // the driver doesn't support quote (e.g. oci)
        return "'" . addcslashes(str_replace("'", "''", $str), "\000\n\r\\\032") . "'";
    }

    public function quoteColumnName(string $name): string
    {
        if (strpos($name, '(') !== false || strpos($name, '[[') !== false) {
            return $name;
        }
        if (($pos = strrpos($name, '.')) !== false) {
            $prefix = $this->quoteTableName(substr($name, 0, $pos)) . '.';
            $name = substr($name, $pos + 1);
        } else {
            $prefix = '';
        }
        if (strpos($name, '{{') !== false) {
            return $name;
        }

        return $prefix . $this->quoteSimpleColumnName($name);
    }

    public function quoteSimpleColumnName(string $name): string
    {
        $startingCharacter = $endingCharacter = $this->columnQuoteCharacter;
        return $name === '*' || strpos(
            $name,
            $startingCharacter
        ) !== false ? $name : $startingCharacter . $name . $endingCharacter;
    }

    public function unquoteSimpleTableName(string $name): string
    {
        $startingCharacter = $this->tableQuoteCharacter;
        return strpos($name, $startingCharacter) === false ? $name : substr($name, 1, -1);
    }

    public function unquoteSimpleColumnName(string $name): string
    {
        $startingCharacter = $this->columnQuoteCharacter;
        return strpos($name, $startingCharacter) === false ? $name : substr($name, 1, -1);
    }

    public function convertException(Throwable $e, string $rawSql): Throwable
    {
        if ($e instanceof Exception) {
            return $e;
        }

        $exceptionClass = Exception::class;
        foreach ($this->exceptionMap as $error => $class) {
            if (strpos($e->getMessage(), $error) !== false) {
                $exceptionClass = $class;
            }
        }
        $message = $e->getMessage() . "\nThe SQL being executed was: $rawSql";
        $errorInfo = $e instanceof PDOException ? $e->errorInfo : null;
        return new $exceptionClass($message, $errorInfo, (int)$e->getCode(), $e);
    }

    public function isReadQuery(string $sql): bool
    {
        $pattern = '/^\s*(SELECT|SHOW|DESCRIBE)\b/i';
        return preg_match($pattern, $sql) > 0;
    }

    public function getServerVersion(): string
    {
        if ($this->_serverVersion === null) {
            $this->_serverVersion = $this->db->getSlavePdo()->getAttribute(\PDO::ATTR_SERVER_VERSION);
        }
        return $this->_serverVersion;
    }

    protected function resolveTableName(string $name): TableSchema
    {
        throw new NotSupportedException(get_class($this) . ' does not support resolving table names.');
    }

    abstract protected function loadTableSchema(string $name): ?TableSchema;

    protected function createColumnSchema(): ColumnSchema
    {
        return create($this->columnSchemaClass, [], false);
    }

    protected function getColumnPhpType(ColumnSchema $column): string
    {
        static $typeMap = [
            // abstract type => php type
            self::TYPE_TINYINT => 'integer',
            self::TYPE_SMALLINT => 'integer',
            self::TYPE_INTEGER => 'integer',
            self::TYPE_BIGINT => 'integer',
            self::TYPE_BOOLEAN => 'boolean',
            self::TYPE_FLOAT => 'double',
            self::TYPE_DOUBLE => 'double',
            self::TYPE_BINARY => 'resource',
            self::TYPE_JSON => 'array',
        ];
        if (isset($typeMap[$column->type])) {
            return $typeMap[$column->type];
        }

        return 'string';
    }

    protected function getCacheTag(): string
    {
        return md5(serialize([
            __CLASS__,
            $this->db->dsn,
            $this->db->username,
        ]));
    }

    protected function normalizePdoRowKeyCase(array $row, bool $multiple): array
    {
        if ($this->db->getSlavePdo()->getAttribute(\PDO::ATTR_CASE) !== \PDO::CASE_UPPER) {
            return $row;
        }

        if ($multiple) {
            return array_map(function (array $row) {
                return array_change_key_case($row, CASE_LOWER);
            }, $row);
        }

        return array_change_key_case($row, CASE_LOWER);
    }
}
