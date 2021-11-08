<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Psr\SimpleCache\CacheInterface;
use Rabbit\Base\Core\BaseObject;

class Query extends BaseObject implements QueryInterface, ExpressionInterface
{
    use QueryTrait;
    use QueryTraitExt;

    public array $select = [];

    public ?string $selectOption = null;

    public bool $distinct = false;

    public ?array $from = null;

    public ?array $groupBy = null;

    public ?array $join = null;

    public null|string|array $having = null;

    public ?array $union = null;

    public array $params = [];

    public ?int $queryCacheDuration = null;

    protected ?CacheInterface $cache = null;

    protected ?\Rabbit\Pool\ConnectionInterface $db = null;

    protected ?int $share = null;

    public function __construct(?\Rabbit\Pool\ConnectionInterface $db = null, array $config = [])
    {
        $this->db = $db ?? getDI('db')->get();
        $config !== [] && configure($this, $config);
    }

    public static function create(Query $from = null): self
    {
        if ($from === null) {
            return new self();
        }
        return new self($from->db, [
            'where' => $from->where,
            'limit' => $from->limit,
            'offset' => $from->offset,
            'orderBy' => $from->orderBy,
            'indexBy' => $from->indexBy,
            'select' => $from->select,
            'selectOption' => $from->selectOption,
            'distinct' => $from->distinct,
            'from' => $from->from,
            'groupBy' => $from->groupBy,
            'join' => $from->join,
            'having' => $from->having,
            'union' => $from->union,
            'params' => $from->params,
            'queryCacheDuration' => $from->queryCacheDuration,
            'cache' => $from->cache,
        ]);
    }

    public function prepare(QueryBuilder $builder): self
    {
        return $this;
    }

    public function batch(int $batchSize = 100): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => $this,
            'batchSize' => $batchSize,
            'db' => $this->db,
            'each' => false,
        ], [], false);
    }

    public function each(int $batchSize = 100): BatchQueryResult
    {
        return create([
            'class' => BatchQueryResult::class,
            'query' => method_exists($this, 'asArray') ? $this : $this,
            'batchSize' => $batchSize,
            'db' => $this->db,
            'each' => true,
        ], [], false);
    }

    public function all(): array
    {
        if ($this->emulateExecution) {
            return [];
        }
        $rows = $this->createCommand()->queryAll();
        $rows = $this->populate($rows);
        return $rows;
    }

    public function createCommand(): Command
    {
        [$sql, $params] = $this->db->getQueryBuilder()->build($this);

        $command = $this->db->createCommand($sql, $params);
        $command->share($this->share);
        $this->setCommandCache($command);

        return $command;
    }

    protected function setCommandCache(Command $command): Command
    {
        if ($this->queryCacheDuration !== null) {
            $duration = $this->queryCacheDuration === true ? null : $this->queryCacheDuration;
            $command->cache($duration, $this->cache);
        }

        return $command;
    }

    public function scalar(): null|float|int|bool|string|array
    {
        if ($this->emulateExecution) {
            return null;
        }

        return $this->createCommand()->queryScalar();
    }

    public function column(): ?array
    {
        if ($this->emulateExecution) {
            return [];
        }

        if ($this->indexBy === null) {
            return $this->createCommand()->queryColumn();
        }

        if (is_string($this->indexBy) && is_array($this->select) && count($this->select) === 1) {
            if (strpos($this->indexBy, '.') === false && count($tables = $this->getTablesUsedInFrom()) > 0) {
                $this->select[] = key($tables) . '.' . $this->indexBy;
            } else {
                $this->select[] = $this->indexBy;
            }
        }
        $rows = $this->createCommand()->queryAll();
        $results = [];
        foreach ($rows as $row) {
            $value = reset($row);

            if ($this->indexBy instanceof \Closure) {
                $results[call_user_func($this->indexBy, $row)] = $value;
            } else {
                $results[$row[$this->indexBy]] = $value;
            }
        }

        return $results;
    }

    public function getTablesUsedInFrom(): array
    {
        if (empty($this->from)) {
            return [];
        }

        if (is_array($this->from)) {
            $tableNames = $this->from;
        } elseif (is_string($this->from)) {
            $tableNames = preg_split('/\s*,\s*/', trim($this->from), -1, PREG_SPLIT_NO_EMPTY);
        } elseif ($this->from instanceof Expression) {
            $tableNames = [$this->from];
        } else {
            throw new \InvalidArgumentException(gettype($this->from) . ' in $from is not supported.');
        }

        return $this->cleanUpTableNames($tableNames);
    }

    protected function cleanUpTableNames(array $tableNames): array
    {
        $cleanedUpTableNames = [];
        foreach ($tableNames as $alias => $tableName) {
            if (is_string($tableName) && !is_string($alias)) {
                $pattern = <<<PATTERN
~
^
\s*
(
(?:['"`\[]|{{)
.*?
(?:['"`\]]|}})
|
\(.*?\)
|
.*?
)
(?:
(?:
    \s+
    (?:as)?
    \s*
)
(
   (?:['"`\[]|{{)
    .*?
    (?:['"`\]]|}})
    |
    .*?
)
)?
\s*
$
~iux
PATTERN;
                if (preg_match($pattern, $tableName, $matches)) {
                    if (isset($matches[2])) {
                        [, $tableName, $alias] = $matches;
                    } else {
                        $tableName = $alias = $matches[1];
                    }
                }
            }


            if ($tableName instanceof Expression) {
                if (!is_string($alias)) {
                    throw new \InvalidArgumentException('To use Expression in from() method, pass it in array format with alias.');
                }
                $cleanedUpTableNames[$this->ensureNameQuoted($alias)] = $tableName;
            } elseif ($tableName instanceof self) {
                $cleanedUpTableNames[$this->ensureNameQuoted($alias)] = $tableName;
            } else {
                $cleanedUpTableNames[$this->ensureNameQuoted($alias)] = $this->ensureNameQuoted($tableName);
            }
        }

        return $cleanedUpTableNames;
    }

    private function ensureNameQuoted(string $name): string
    {
        $name = str_replace(["'", '"', '`', '[', ']'], '', $name);
        if ($name && !preg_match('/^{{.*}}$/', $name)) {
            return '{{' . $name . '}}';
        }

        return $name;
    }

    public function count(string $q = '*'): int
    {
        if ($this->emulateExecution) {
            return 0;
        }

        return (int)$this->queryScalar("COUNT($q)");
    }

    protected function queryScalar(string $selectExpression): null|float|int|bool|string
    {
        if ($this->emulateExecution) {
            return null;
        }

        if (
            !$this->distinct
            && empty($this->groupBy)
            && empty($this->having)
            && empty($this->union)
        ) {
            $select = $this->select;
            $order = $this->orderBy;
            $limit = $this->limit;
            $offset = $this->offset;

            $this->select = [$selectExpression];
            $this->orderBy = null;
            $this->limit = null;
            $this->offset = null;
            $command = $this->createCommand();

            $this->select = $select;
            $this->orderBy = $order;
            $this->limit = $limit;
            $this->offset = $offset;

            return $command->queryScalar();
        }

        $command = (new self($this->db))
            ->select([$selectExpression])
            ->from(['c' => $this])
            ->createCommand();
        $command->share($this->share);
        $this->setCommandCache($command);

        return $command->queryScalar();
    }

    public function from(array $tables): self
    {
        $this->from = $tables;
        return $this;
    }


    public function select(array $columns, string $option = null): self
    {
        // this sequantial assignment is needed in order to make sure select is being reset
        // before using getUniqueColumns() that checks it
        $this->select = [];
        $this->select = $this->getUniqueColumns($columns);
        $this->selectOption = $option;
        return $this;
    }

    protected function getUniqueColumns(array $columns): array
    {
        $unaliasedColumns = $this->getUnaliasedColumnsFromSelect();

        $result = [];
        foreach ($columns as $columnAlias => $columnDefinition) {
            if (!$columnDefinition instanceof Query) {
                if (is_string($columnAlias)) {
                    $existsInSelect = isset($this->select[$columnAlias]) && $this->select[$columnAlias] === $columnDefinition;
                    if ($existsInSelect) {
                        continue;
                    }
                } elseif (is_int($columnAlias)) {
                    $existsInSelect = in_array($columnDefinition, $unaliasedColumns, true);
                    $existsInResultSet = in_array($columnDefinition, $result, true);
                    if ($existsInSelect || $existsInResultSet) {
                        continue;
                    }
                }
            }

            $result[$columnAlias] = $columnDefinition;
        }
        return $result;
    }

    protected function getUnaliasedColumnsFromSelect(): array
    {
        $result = [];
        if (is_array($this->select)) {
            foreach ($this->select as $name => $value) {
                if (is_int($name)) {
                    $result[] = $value;
                }
            }
        }
        return array_unique($result);
    }

    public function sum(string $q): float
    {
        if ($this->emulateExecution) {
            return 0;
        }

        return (float)$this->queryScalar("SUM($q)");
    }

    public function average(string $q): float
    {
        if ($this->emulateExecution) {
            return 0;
        }

        return (float)$this->queryScalar("AVG($q)");
    }

    public function min(string $q): ?string
    {
        return $this->queryScalar("MIN($q)");
    }

    public function max(string $q): ?string
    {
        return $this->queryScalar("MAX($q)");
    }

    public function exists(): bool
    {
        if ($this->emulateExecution) {
            return false;
        }
        $command = $this->createCommand();
        $params = $command->params;
        $command->setSql($command->db->getQueryBuilder()->selectExists($command->getSql()));
        $command->bindValues($params);
        return (bool)$command->queryScalar();
    }

    public function addSelect(array $columns): self
    {
        $columns = $this->getUniqueColumns($columns);
        if ($this->select === null) {
            $this->select = $columns;
        } else {
            $this->select = array_merge($this->select, $columns);
        }

        return $this;
    }

    public function distinct(bool $value = true): self
    {
        $this->distinct = $value;
        return $this;
    }

    public function where(string|array|ExpressionInterface $condition, array $params = []): self
    {
        $this->where = $condition;
        $this->addParams($params);
        return $this;
    }

    public function addParams(array $params): self
    {
        if (!empty($params)) {
            if (empty($this->params)) {
                $this->params = $params;
            } else {
                foreach ($params as $name => $value) {
                    if (is_int($name)) {
                        $this->params[] = $value;
                    } else {
                        $this->params[$name] = $value;
                    }
                }
            }
        }

        return $this;
    }

    public function andWhere(string|array|ExpressionInterface $condition, array $params = []): self
    {
        if ($this->where === null) {
            $this->where = $condition;
        } elseif (is_array($this->where) && isset($this->where[0]) && strcasecmp($this->where[0], 'and') === 0) {
            $this->where[] = $condition;
        } else {
            $this->where = ['and', $this->where, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    public function orWhere(string|array|ExpressionInterface $condition, array $params = []): self
    {
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['or', $this->where, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    public function andFilterCompare(string $name, string $value, string $defaultOperator = '='): self
    {
        if (preg_match('/^(<>|>=|>|<=|<|=)/', $value, $matches)) {
            $operator = $matches[1];
            $value = substr($value, strlen($operator));
        } else {
            $operator = $defaultOperator;
        }

        return $this->andFilterWhere([$operator, $name, $value]);
    }

    public function join(string|array $type, $table, string $on = '', array $params = []): self
    {
        $this->join[] = [$type, $table, $on];
        return $this->addParams($params);
    }

    public function innerJoin(string|array $table, string $on = '', array $params = []): self
    {
        $this->join[] = ['INNER JOIN', $table, $on];
        return $this->addParams($params);
    }

    public function leftJoin(string|array $table, string $on = '', array $params = []): self
    {
        $this->join[] = ['LEFT JOIN', $table, $on];
        return $this->addParams($params);
    }

    public function rightJoin(string|array $table, string $on = '', array $params = []): self
    {
        $this->join[] = ['RIGHT JOIN', $table, $on];
        return $this->addParams($params);
    }

    public function groupBy(string|array|ExpressionInterface $columns): self
    {
        if ($columns instanceof ExpressionInterface) {
            $columns = [$columns];
        } elseif (!is_array($columns)) {
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        $this->groupBy = $columns;
        return $this;
    }

    public function addGroupBy(string|array $columns): self
    {
        if ($columns instanceof ExpressionInterface) {
            $columns = [$columns];
        } elseif (!is_array($columns)) {
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
        }
        if ($this->groupBy === null) {
            $this->groupBy = $columns;
        } else {
            $this->groupBy = array_merge($this->groupBy, $columns);
        }

        return $this;
    }

    public function filterHaving(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->having($condition);
        }

        return $this;
    }

    public function having(string|array|ExpressionInterface $condition, array $params = []): self
    {
        $this->having = $condition;
        $this->addParams($params);
        return $this;
    }

    public function andFilterHaving(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->andHaving($condition);
        }

        return $this;
    }

    public function andHaving(string|array|ExpressionInterface $condition, array $params = []): self
    {
        if ($this->having === null) {
            $this->having = $condition;
        } else {
            $this->having = ['and', $this->having, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    public function orFilterHaving(array $condition): self
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->orHaving($condition);
        }

        return $this;
    }

    public function orHaving(string|array|ExpressionInterface $condition, array $params = []): self
    {
        if ($this->having === null) {
            $this->having = $condition;
        } else {
            $this->having = ['or', $this->having, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    public function union(string|Query $sql, bool $all = false): self
    {
        $this->union[] = ['query' => $sql, 'all' => $all];
        return $this;
    }

    public function params(array $params): self
    {
        $this->params = $params;
        return $this;
    }

    public function cache(int $duration = 0, ?CacheInterface $cache = null): self
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

    public function __toString()
    {
        return serialize($this);
    }

    public function __call($name, $arguments)
    {
        $arguments = array_shift($arguments);
        if (strpos($name, '@') !== false && substr_count($name, '@') === 1) {
            [$method, $function] = explode('@', $name);
            if (in_array($method, ['select', 'where'])) {
                switch ($method) {
                    case 'select':
                        $this->$method = array_merge(
                            $this->$method ?? [],
                            [sprintf("%s(%s)", $function, implode(',', $arguments))]
                        );
                        break;
                    case 'where':
                        $this->{'and' . $method}(sprintf("%s(%s)", $function, implode(',', $arguments)));
                        break;
                }
            }
        }
        return $this;
    }
}
