<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;


use Generator;
use InvalidArgumentException;
use Rabbit\Base\Helper\StringHelper;
use Rabbit\DB\Conditions\HashCondition;
use Rabbit\DB\Conditions\SimpleCondition;
use Rabbit\DB\Conditions\ConditionInterface;
use Rabbit\Base\Exception\NotSupportedException;

class QueryBuilder
{
    public string $separator = ' ';

    protected array $typeMap = [];

    protected array $conditionClasses = [];

    protected array $expressionBuilders = [];

    public function __construct(public ConnectionInterface $db)
    {
        $this->expressionBuilders = $this->defaultExpressionBuilders();
        $this->conditionClasses = $this->defaultConditionClasses();
    }

    protected function defaultExpressionBuilders(): array
    {
        return [
            Query::class => QueryExpressionBuilder::class,
            PdoValue::class => PdoValueBuilder::class,
            Expression::class => ExpressionBuilder::class,
            Conditions\ConjunctionCondition::class => Conditions\ConjunctionConditionBuilder::class,
            Conditions\NotCondition::class => Conditions\NotConditionBuilder::class,
            Conditions\AndCondition::class => Conditions\ConjunctionConditionBuilder::class,
            Conditions\OrCondition::class => Conditions\ConjunctionConditionBuilder::class,
            Conditions\BetweenCondition::class => Conditions\BetweenConditionBuilder::class,
            Conditions\InCondition::class => Conditions\InConditionBuilder::class,
            Conditions\LikeCondition::class => Conditions\LikeConditionBuilder::class,
            Conditions\ExistsCondition::class => Conditions\ExistsConditionBuilder::class,
            Conditions\SimpleCondition::class => Conditions\SimpleConditionBuilder::class,
            Conditions\HashCondition::class => Conditions\HashConditionBuilder::class,
            Conditions\BetweenColumnsCondition::class => Conditions\BetweenColumnsConditionBuilder::class,
        ];
    }

    protected function defaultConditionClasses(): array
    {
        return [
            'NOT' => Conditions\NotCondition::class,
            'AND' => Conditions\AndCondition::class,
            'OR' => Conditions\OrCondition::class,
            'BETWEEN' => Conditions\BetweenCondition::class,
            'NOT BETWEEN' => Conditions\BetweenCondition::class,
            'IN' => Conditions\InCondition::class,
            'NOT IN' => Conditions\InCondition::class,
            'LIKE' => Conditions\LikeCondition::class,
            'NOT LIKE' => Conditions\LikeCondition::class,
            'OR LIKE' => Conditions\LikeCondition::class,
            'OR NOT LIKE' => Conditions\LikeCondition::class,
            'EXISTS' => Conditions\ExistsCondition::class,
            'NOT EXISTS' => Conditions\ExistsCondition::class,
        ];
    }

    public function setExpressionBuilders(array $builders): void
    {
        $this->expressionBuilders = [...$this->expressionBuilders, ...$builders];
    }

    public function setConditionClasses(array $classes): void
    {
        $this->conditionClasses = [...$this->conditionClasses, ...$classes];
    }

    public function insert(string $table, array|Query $columns, array &$params = [], bool $withUpdate = false): string
    {
        [$names, $placeholders, $values, $params] = $this->prepareInsertValues($table, $columns, $params);
        $sql = 'INSERT INTO ' . $this->db->quoteTableName($table)
            . (!empty($names) ? ' (' . implode(', ', $names) . ')' : '')
            . (!empty($placeholders) ? ' VALUES (' . implode(', ', $placeholders) . ')' : $values);
        if ($withUpdate) {
            $updates = [];
            foreach ($names as $name) {
                $updates[] = "{$name}=values($name)";
            }
            $sql .= " on duplicate key update " . implode(', ', $updates);
        }
        return $sql;
    }

    protected function prepareInsertValues(string $table, array|Query $columns, array $params = []): array
    {
        $schema = $this->db->getSchema();
        $tableSchema = $schema->getTableSchema($table);
        $columnSchemas = $tableSchema !== null ? $tableSchema->columns : [];
        $names = [];
        $placeholders = [];
        $values = ' DEFAULT VALUES';
        if ($columns instanceof Query) {
            [$names, $values, $params] = $this->prepareInsertSelectSubQuery($columns, $schema, $params);
        } else {
            foreach ($columns as $name => $value) {
                $names[] = $schema->quoteColumnName($name);
                $value = isset($columnSchemas[$name]) ? $columnSchemas[$name]->dbTypecast($value) : $value;

                if ($value instanceof ExpressionInterface) {
                    $placeholders[] = $this->buildExpression($value, $params);
                } elseif ($value instanceof Query) {
                    [$sql, $params] = $this->build($value, $params);
                    $placeholders[] = "($sql)";
                } else {
                    $placeholders[] = $this->bindParam($value, $params);
                }
            }
        }
        return [$names, $placeholders, $values, $params];
    }

    protected function prepareInsertSelectSubQuery(Query $columns, Schema $schema, array $params = []): array
    {
        if (!is_array($columns->select) || empty($columns->select) || in_array('*', $columns->select)) {
            throw new InvalidArgumentException('Expected select query object with enumerated (named) parameters');
        }

        [$values, $params] = $this->build($columns, $params);
        $names = [];
        $values = ' ' . $values;
        foreach ($columns->select as $title => $field) {
            if (is_string($title)) {
                $names[] = $schema->quoteColumnName($title);
            } elseif (preg_match('/^(.*?)(?i:\s+as\s+|\s+)([\w\-_\.]+)$/', $field, $matches)) {
                $names[] = $schema->quoteColumnName($matches[2]);
            } else {
                $names[] = $schema->quoteColumnName($field);
            }
        }

        return [$names, $values, $params];
    }

    public function build(Query $query, array $params = []): array
    {
        $query = $query->prepare($this);

        $params = [...$params, ...$query->params];

        $clauses = [
            $this->buildSelect($query->select, $params, $query->distinct, $query->selectOption),
            $this->buildFrom($query->from, $params),
            $this->buildJoin($query->join, $params),
            $this->buildWhere($query->where, $params),
            $this->buildGroupBy($query->groupBy, $params),
            $this->buildHaving($query->having, $params),
        ];

        $sql = implode($this->separator, array_filter($clauses));
        $sql = $this->buildOrderByAndLimit($sql, $query->orderBy, $query->limit, $query->offset, $params);

        $union = $this->buildUnion($query->union, $params);
        if ($union !== '') {
            $sql = "($sql){$this->separator}$union";
        }

        return [$sql, $params];
    }

    public function buildSelect(array $columns, array &$params, bool $distinct = false, string $selectOption = null)
    {
        $select = $distinct ? 'SELECT DISTINCT' : 'SELECT';
        if ($selectOption !== null) {
            $select .= ' ' . $selectOption;
        }

        if (empty($columns)) {
            return $select . ' *';
        }

        foreach ($columns as $i => $column) {
            if ($column instanceof ExpressionInterface) {
                if (is_int($i)) {
                    $columns[$i] = $this->buildExpression($column, $params);
                } else {
                    $columns[$i] = $this->buildExpression($column, $params) . ' AS ' . $this->db->quoteColumnName($i);
                }
            } elseif ($column instanceof Query) {
                [$sql, $params] = $this->build($column, $params);
                $columns[$i] = "($sql) AS " . $this->db->quoteColumnName($i);
            } elseif (is_string($i)) {
                if (strpos($column, '(') === false) {
                    $column = $this->db->quoteColumnName($column);
                }
                $columns[$i] = "$column AS " . $this->db->quoteColumnName($i);
            } elseif (strpos($column, '(') === false) {
                if (preg_match('/^(.*?)(?i:\s+as\s+|\s+)([\w\-_\.]+)$/', $column, $matches)) {
                    $columns[$i] = $this->db->quoteColumnName($matches[1]) . ' AS ' . $this->db->quoteColumnName($matches[2]);
                } else {
                    $columns[$i] = $this->db->quoteColumnName($column);
                }
            }
        }

        return $select . ' ' . implode(', ', $columns);
    }

    public function buildExpression(ExpressionInterface $expression, array &$params = []): string
    {
        $builder = $this->getExpressionBuilder($expression);

        return $builder->build($expression, $params);
    }

    public function getExpressionBuilder(ExpressionInterface $expression): ExpressionBuilderInterface
    {
        $className = get_class($expression);

        if (!isset($this->expressionBuilders[$className])) {
            foreach (array_reverse($this->expressionBuilders) as $expressionClass => $builderClass) {
                if (is_subclass_of($expression, $expressionClass)) {
                    $this->expressionBuilders[$className] = $builderClass;
                    break;
                }
            }

            if (!isset($this->expressionBuilders[$className])) {
                throw new InvalidArgumentException('Expression of class ' . $className . ' can not be built in ' . get_class($this));
            }
        }

        if (!is_object($this->expressionBuilders[$className])) {
            $this->expressionBuilders[$className] = new $this->expressionBuilders[$className]($this);
        }

        return $this->expressionBuilders[$className];
    }

    public function buildFrom(array $tables, array &$params): string
    {
        if (empty($tables)) {
            return '';
        }

        $tables = $this->quoteTableNames($tables, $params);

        return 'FROM ' . implode(', ', $tables);
    }

    private function quoteTableNames(array $tables, array &$params)
    {
        foreach ($tables as $i => $table) {
            if ($table instanceof Query) {
                [$sql, $params] = $this->build($table, $params);
                $tables[$i] = "($sql) " . $this->db->quoteTableName($i);
            } elseif (is_string($i)) {
                if (strpos($table, '(') === false) {
                    $table = $this->db->quoteTableName($table);
                }
                $tables[$i] = "$table " . $this->db->quoteTableName($i);
            } elseif (strpos($table, '(') === false) {
                if (preg_match('/^(.*?)(?i:\s+as|)\s+([^ ]+)$/', $table, $matches)) { // with alias
                    $tables[$i] = $this->db->quoteTableName($matches[1]) . ' ' . $this->db->quoteTableName($matches[2]);
                } else {
                    $tables[$i] = $this->db->quoteTableName($table);
                }
            }
        }

        return $tables;
    }

    public function buildJoin(?array $joins, array &$params): string
    {
        if (empty($joins)) {
            return '';
        }

        foreach ($joins as $i => $join) {
            if (!is_array($join) || !isset($join[0], $join[1])) {
                throw new Exception('A join clause must be specified as an array of join type, join table, and optionally join condition.');
            }
            // 0:join type, 1:join table, 2:on-condition (optional)
            [$joinType, $table] = $join;
            $tables = $this->quoteTableNames((array)$table, $params);
            $table = reset($tables);
            $joins[$i] = "$joinType $table";
            if (isset($join[2])) {
                $condition = $this->buildCondition($join[2], $params);
                if ($condition !== '') {
                    $joins[$i] .= ' ON ' . $condition;
                }
            }
        }

        return implode($this->separator, $joins);
    }

    public function buildCondition(null|string|array|ExpressionInterface $condition, array &$params): string
    {
        if (is_array($condition)) {
            if (empty($condition)) {
                return '';
            }

            $condition = $this->createConditionFromArray($condition);
        }

        if ($condition instanceof ExpressionInterface) {
            return $this->buildExpression($condition, $params);
        }

        return (string)$condition;
    }

    public function createConditionFromArray(array $condition): ConditionInterface
    {
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            $operator = strtoupper(array_shift($condition));
            if (isset($this->conditionClasses[$operator])) {
                $className = $this->conditionClasses[$operator];
            } else {
                $className = SimpleCondition::class;
            }
            /** @var ConditionInterface $className */
            return $className::fromArrayDefinition($operator, $condition);
        }

        // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
        return new HashCondition($condition);
    }

    public function buildWhere(null|string|array $condition, array &$params): string
    {
        $where = $this->buildCondition($condition, $params);

        return $where === '' ? '' : 'WHERE ' . $where;
    }

    public function buildGroupBy(?array $columns, array &$params): string
    {
        if (empty($columns)) {
            return '';
        }
        foreach ($columns as $i => $column) {
            if ($column instanceof ExpressionInterface) {
                $columns[$i] = $this->buildExpression($column);
                $params = [...$params, ...$column->params];
            } elseif (strpos($column, '(') === false) {
                $columns[$i] = $this->db->quoteColumnName($column);
            }
        }

        return 'GROUP BY ' . implode(', ', $columns);
    }

    public function buildHaving(null|string|array $condition, array &$params): string
    {
        $having = $this->buildCondition($condition, $params);

        return $having === '' ? '' : 'HAVING ' . $having;
    }

    public function buildOrderByAndLimit(string $sql, ?array $orderBy, ?int $limit, ?int $offset, array &$params): string
    {
        $orderBy = $this->buildOrderBy($orderBy, $params);
        if ($orderBy !== '') {
            $sql .= $this->separator . $orderBy;
        }
        $limit = $this->buildLimit($limit, $offset);
        if ($limit !== '') {
            $sql .= $this->separator . $limit;
        }

        return $sql;
    }

    public function buildOrderBy(?array $columns, array &$params): string
    {
        if (empty($columns)) {
            return '';
        }
        $orders = [];
        foreach ($columns as $name => $direction) {
            if ($direction instanceof ExpressionInterface) {
                $orders[] = $this->buildExpression($direction);
                $params = [...$params, ...$direction->params];
            } else {
                $orders[] = $this->db->quoteColumnName($name) . ($direction === SORT_DESC ? ' DESC' : '');
            }
        }

        return 'ORDER BY ' . implode(', ', $orders);
    }

    public function buildLimit(?int $limit, ?int $offset): string
    {
        $sql = '';
        if ($this->hasLimit($limit)) {
            $sql = 'LIMIT ' . $limit;
        }
        if ($this->hasOffset($offset)) {
            $sql .= ' OFFSET ' . $offset;
        }

        return ltrim($sql);
    }

    protected function hasLimit(ExpressionInterface|string|int|null $limit): bool
    {
        return ($limit instanceof ExpressionInterface) || ctype_digit((string)$limit);
    }

    protected function hasOffset(ExpressionInterface|string|int|null $offset): bool
    {
        return ($offset instanceof ExpressionInterface) || ctype_digit((string)$offset) && (string)$offset !== '0';
    }

    public function buildUnion(?array $unions, array &$params): string
    {
        if (empty($unions)) {
            return '';
        }

        $result = '';

        foreach ($unions as $i => $union) {
            $query = $union['query'];
            if ($query instanceof Query) {
                [$unions[$i]['query'], $params] = $this->build($query, $params);
            }

            $result .= 'UNION ' . ($union['all'] ? 'ALL ' : '') . '( ' . $unions[$i]['query'] . ' ) ';
        }

        return trim($result);
    }

    public function bindParam(int|float|bool|array|string|null $value, array &$params): string
    {
        $phName = '?';
        $params[count($params)] = $value;

        return $phName;
    }

    public function batchInsert(string $table, array $columns, array|Generator $rows, array &$params = []): string
    {
        if (empty($rows)) {
            return '';
        }

        $schema = $this->db->getSchema();
        if (($tableSchema = $schema->getTableSchema($table)) !== null) {
            $columnSchemas = $tableSchema->columns;
        } else {
            $columnSchemas = [];
        }

        $values = [];
        foreach ($rows as $row) {
            $vs = [];
            foreach ($row as $i => $value) {
                if (isset($columns[$i], $columnSchemas[$columns[$i]])) {
                    $value = $columnSchemas[$columns[$i]]->dbTypecast($value);
                }
                if (is_string($value)) {
                    $value = $schema->quoteValue($value);
                } elseif (is_float($value)) {
                    // ensure type cast always has . as decimal separator in all locales
                    $value = StringHelper::floatToString($value);
                } elseif ($value === false) {
                    $value = 0;
                } elseif ($value === null) {
                    $value = 'NULL';
                } elseif ($value instanceof ExpressionInterface) {
                    $value = $this->buildExpression($value, $params);
                }
                $vs[] = $value;
            }
            $values[] = '(' . implode(', ', $vs) . ')';
        }
        if (empty($values)) {
            return '';
        }

        foreach ($columns as $i => $name) {
            $columns[$i] = $schema->quoteColumnName($name);
        }

        return 'INSERT INTO ' . $schema->quoteTableName($table)
            . ' (' . implode(', ', $columns) . ') VALUES ' . implode(', ', $values);
    }

    public function upsert(string $table, array|Query $insertColumns, array|bool $updateColumns, array &$params = []): string
    {
        throw new NotSupportedException($this->db->getDriverName() . ' does not support upsert statements.');
    }

    public function update(string $table, array $columns, array|string $condition, array &$params = []): string
    {
        [$lines, $params] = $this->prepareUpdateSets($table, $columns, $params);
        $sql = 'UPDATE ' . $this->db->quoteTableName($table) . ' SET ' . implode(', ', $lines);
        $where = $this->buildWhere($condition, $params);
        return $where === '' ? $sql : $sql . ' ' . $where;
    }

    protected function prepareUpdateSets(string $table, array $columns, array $params = []): array
    {
        $tableSchema = $this->db->getTableSchema($table);
        $columnSchemas = $tableSchema !== null ? $tableSchema->columns : [];
        $sets = [];
        foreach ($columns as $name => $value) {
            $value = isset($columnSchemas[$name]) ? $columnSchemas[$name]->dbTypecast($value) : $value;
            if ($value instanceof ExpressionInterface) {
                $placeholder = $this->buildExpression($value, $params);
            } else {
                $placeholder = $this->bindParam($value, $params);
            }

            $sets[] = $this->db->quoteColumnName($name) . '=' . $placeholder;
        }
        return [$sets, $params];
    }

    public function delete(string $table, array|string $condition, array &$params): string
    {
        $sql = 'DELETE FROM ' . $this->db->quoteTableName($table);
        $where = $this->buildWhere($condition, $params);

        return $where === '' ? $sql : $sql . ' ' . $where;
    }

    public function createTable(string $table, array $columns, string $options = null): string
    {
        $cols = [];
        foreach ($columns as $name => $type) {
            if (is_string($name)) {
                $cols[] = "\t" . $this->db->quoteColumnName($name) . ' ' . $this->getColumnType($type);
            } else {
                $cols[] = "\t" . $type;
            }
        }
        $sql = 'CREATE TABLE ' . $this->db->quoteTableName($table) . " (\n" . implode(",\n", $cols) . "\n)";

        return $options === null ? $sql : $sql . ' ' . $options;
    }

    public function getColumnType(string|ColumnSchemaBuilder $type): string
    {
        if ($type instanceof ColumnSchemaBuilder) {
            $type = $type->__toString();
        }

        if (isset($this->typeMap[$type])) {
            return $this->typeMap[$type];
        } elseif (preg_match('/^(\w+)\((.+?)\)(.*)$/', $type, $matches)) {
            if (isset($this->typeMap[$matches[1]])) {
                return preg_replace('/\(.+\)/', '(' . $matches[2] . ')', $this->typeMap[$matches[1]]) . $matches[3];
            }
        } elseif (preg_match('/^(\w+)\s+/', $type, $matches)) {
            if (isset($this->typeMap[$matches[1]])) {
                return preg_replace('/^\w+/', $this->typeMap[$matches[1]], $type);
            }
        }

        return $type;
    }

    public function renameTable(string $oldName, string $newName): string
    {
        return 'RENAME TABLE ' . $this->db->quoteTableName($oldName) . ' TO ' . $this->db->quoteTableName($newName);
    }

    public function dropTable(string $table): string
    {
        return 'DROP TABLE ' . $this->db->quoteTableName($table);
    }

    public function addPrimaryKey(string $name, string $table, string|array $columns): string
    {
        if (is_string($columns)) {
            $columns = preg_split('/\s*,\s*/', $columns, -1, PREG_SPLIT_NO_EMPTY);
        }

        foreach ($columns as $i => $col) {
            $columns[$i] = $this->db->quoteColumnName($col);
        }

        return 'ALTER TABLE ' . $this->db->quoteTableName($table) . ' ADD CONSTRAINT '
            . $this->db->quoteColumnName($name) . ' PRIMARY KEY ('
            . implode(', ', $columns) . ')';
    }

    public function dropPrimaryKey(string $name, string $table): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' DROP CONSTRAINT ' . $this->db->quoteColumnName($name);
    }

    public function truncateTable(string $table): string
    {
        return 'TRUNCATE TABLE ' . $this->db->quoteTableName($table);
    }

    public function addColumn(string $table, string $column, string $type): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' ADD ' . $this->db->quoteColumnName($column) . ' '
            . $this->getColumnType($type);
    }

    public function dropColumn(string $table, string $column): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' DROP COLUMN ' . $this->db->quoteColumnName($column);
    }

    public function renameColumn(string $table, string $oldName, string $newName): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' RENAME COLUMN ' . $this->db->quoteColumnName($oldName)
            . ' TO ' . $this->db->quoteColumnName($newName);
    }

    public function alterColumn(string $table, string $column, string $type): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table) . ' CHANGE '
            . $this->db->quoteColumnName($column) . ' '
            . $this->db->quoteColumnName($column) . ' '
            . $this->getColumnType($type);
    }

    public function addForeignKey(string $name, string $table, string|array $columns, string $refTable, string|array $refColumns, string $delete = null, string $update = null): string
    {
        $sql = 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' ADD CONSTRAINT ' . $this->db->quoteColumnName($name)
            . ' FOREIGN KEY (' . $this->buildColumns($columns) . ')'
            . ' REFERENCES ' . $this->db->quoteTableName($refTable)
            . ' (' . $this->buildColumns($refColumns) . ')';
        if ($delete !== null) {
            $sql .= ' ON DELETE ' . $delete;
        }
        if ($update !== null) {
            $sql .= ' ON UPDATE ' . $update;
        }

        return $sql;
    }

    public function buildColumns(string|array $columns): string
    {
        if (!is_array($columns)) {
            if (strpos($columns, '(') !== false) {
                return $columns;
            }

            $rawColumns = $columns;
            $columns = preg_split('/\s*,\s*/', $columns, -1, PREG_SPLIT_NO_EMPTY);
            if ($columns === false) {
                throw new InvalidArgumentException("$rawColumns is not valid columns.");
            }
        }
        foreach ($columns as $i => $column) {
            if ($column instanceof ExpressionInterface) {
                $columns[$i] = $this->buildExpression($column);
            } elseif (strpos($column, '(') === false) {
                $columns[$i] = $this->db->quoteColumnName($column);
            }
        }

        return implode(', ', $columns);
    }

    public function dropForeignKey(string $name, string $table): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' DROP CONSTRAINT ' . $this->db->quoteColumnName($name);
    }

    public function createIndex(string $name, string $table, string|array $columns, bool $unique = false): string
    {
        return ($unique ? 'CREATE UNIQUE INDEX ' : 'CREATE INDEX ')
            . $this->db->quoteTableName($name) . ' ON '
            . $this->db->quoteTableName($table)
            . ' (' . $this->buildColumns($columns) . ')';
    }

    public function dropIndex(string $name, string $table): string
    {
        return 'DROP INDEX ' . $this->db->quoteTableName($name) . ' ON ' . $this->db->quoteTableName($table);
    }

    public function addUnique(string $name, string $table, string|array $columns): string
    {
        if (is_string($columns)) {
            $columns = preg_split('/\s*,\s*/', $columns, -1, PREG_SPLIT_NO_EMPTY);
        }
        foreach ($columns as $i => $col) {
            $columns[$i] = $this->db->quoteColumnName($col);
        }

        return 'ALTER TABLE ' . $this->db->quoteTableName($table) . ' ADD CONSTRAINT '
            . $this->db->quoteColumnName($name) . ' UNIQUE ('
            . implode(', ', $columns) . ')';
    }

    public function dropUnique(string $name, string $table): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' DROP CONSTRAINT ' . $this->db->quoteColumnName($name);
    }

    public function addCheck(string $name, string $table, string $expression): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table) . ' ADD CONSTRAINT '
            . $this->db->quoteColumnName($name) . ' CHECK (' . $this->db->quoteSql($expression) . ')';
    }

    public function dropCheck(string $name, string $table): string
    {
        return 'ALTER TABLE ' . $this->db->quoteTableName($table)
            . ' DROP CONSTRAINT ' . $this->db->quoteColumnName($name);
    }

    public function resetSequence(string $table, array|string $value = null): string
    {
        throw new NotSupportedException($this->db->getDriverName() . ' does not support resetting sequence.');
    }

    public function checkIntegrity(string $schema = '', string $table = '', bool $check = true): string
    {
        throw new NotSupportedException($this->db->getDriverName() . ' does not support enabling/disabling integrity check.');
    }

    public function addCommentOnColumn(string $table, string $column, string $comment): string
    {
        return 'COMMENT ON COLUMN ' . $this->db->quoteTableName($table) . '.' . $this->db->quoteColumnName($column) . ' IS ' . $this->db->quoteValue($comment);
    }

    public function addCommentOnTable(string $table, string $comment): string
    {
        return 'COMMENT ON TABLE ' . $this->db->quoteTableName($table) . ' IS ' . $this->db->quoteValue($comment);
    }

    public function dropCommentFromColumn(string $table, string $column): string
    {
        return 'COMMENT ON COLUMN ' . $this->db->quoteTableName($table) . '.' . $this->db->quoteColumnName($column) . ' IS NULL';
    }

    public function dropCommentFromTable(string $table): string
    {
        return 'COMMENT ON TABLE ' . $this->db->quoteTableName($table) . ' IS NULL';
    }

    public function createView(string $viewName, string|Query $subQuery): string
    {
        if ($subQuery instanceof Query) {
            [$rawQuery, $params] = $this->build($subQuery);
            array_walk(
                $params,
                function (mixed &$param): void {
                    $param = $this->db->quoteValue($param);
                }
            );
            $subQuery = strtr($rawQuery, $params);
        }

        return 'CREATE VIEW ' . $this->db->quoteTableName($viewName) . ' AS ' . $subQuery;
    }

    public function dropView(string $viewName): string
    {
        return 'DROP VIEW ' . $this->db->quoteTableName($viewName);
    }

    public function selectExists(string $rawSql): string
    {
        return 'SELECT EXISTS(' . $rawSql . ')';
    }

    protected function prepareUpsertColumns(string $table, $insertColumns, array|Query $updateColumns, array &$constraints = []): array
    {
        if ($insertColumns instanceof Query) {
            [$insertNames] = $this->prepareInsertSelectSubQuery($insertColumns, $this->db->getSchema());
        } else {
            $insertNames = array_map([$this->db, 'quoteColumnName'], array_keys($insertColumns));
        }
        $uniqueNames = $this->getTableUniqueColumnNames($table, $insertNames, $constraints);
        $uniqueNames = array_map([$this->db, 'quoteColumnName'], $uniqueNames);
        if ($updateColumns !== true) {
            return [$uniqueNames, $insertNames, null];
        }

        return [$uniqueNames, $insertNames, array_diff($insertNames, $uniqueNames)];
    }

    private function getTableUniqueColumnNames(string $name, array $columns, array &$constraints = []): array
    {
        $schema = $this->db->getSchema();
        if (!$schema instanceof ConstraintFinderInterface) {
            return [];
        }

        $constraints = [];
        $primaryKey = $schema->getTablePrimaryKey($name);
        if ($primaryKey !== null) {
            $constraints[] = $primaryKey;
        }
        foreach ($schema->getTableIndexes($name) as $constraint) {
            if ($constraint->isUnique) {
                $constraints[] = $constraint;
            }
        }
        $constraints = [...$constraints, ...$schema->getTableUniques($name)];
        // Remove duplicates
        $constraints = array_combine(array_map(function (Constraint $constraint): string|false {
            $columns = $constraint->columnNames;
            sort($columns, SORT_STRING);
            return json_encode($columns);
        }, $constraints), $constraints);
        $columnNames = [];
        // Remove all constraints which do not cover the specified column list
        $constraints = array_values(array_filter(
            $constraints,
            function (Constraint $constraint) use ($schema, $columns, &$columnNames): bool {
                $constraintColumnNames = array_map([$schema, 'quoteColumnName'], $constraint->columnNames);
                $result = !array_diff($constraintColumnNames, $columns);
                if ($result) {
                    $columnNames = [...$columnNames, ...$constraintColumnNames];
                }
                return $result;
            }
        ));
        return array_unique($columnNames);
    }
}
