<?php


namespace rabbit\db;

use rabbit\helper\StringHelper;

/**'
 * Class BatchInsert
 * @package rabbit\db
 */
class BatchInsert
{
    /** @var string */
    private $table;
    /** @var array */
    private $columnSchemas = [];
    /** @var */
    private $sql;
    /** @var ConnectionInterface */
    private $db;
    /** @var bool */
    private $hasRows = 0;
    private $schema;
    /** @var array */
    private $columns = [];

    /**
     * BatchInsert constructor.
     * @param string $table
     * @param array $columns
     * @param ConnectionInterface $db
     */
    public function __construct(string $table, ConnectionInterface $db)
    {
        $this->table = $table;
        $this->db = $db;
        $this->schema = $this->db->getSchema();
        $this->sql = 'INSERT INTO ' . $this->schema->quoteTableName($table);
    }

    /**
     * @param array $columns
     * @return bool
     */
    public function addColumns(array $columns): bool
    {
        if (empty($columns)) {
            return false;
        }

        if (($tableSchema = $this->schema->getTableSchema($this->table)) !== null) {
            $this->columnSchemas = $tableSchema->columns;
        }

        foreach ($columns as $i => $name) {
            $columns[$i] = $this->schema->quoteColumnName($name);
        }
        $this->sql .= ' (' . implode(', ', $columns) . ') VALUES ';
        $this->columns = $columns;
        return true;
    }

    /**
     * @param array $rows
     * @param bool $checkFields
     * @return bool
     */
    public function addRow(array $rows, bool $checkFields = true): bool
    {
        if (empty($rows)) {
            return false;
        }
        $this->hasRows++;
        if ($checkFields) {
            foreach ($rows as $i => $value) {
                if (isset($this->columns[$i], $this->columnSchemas[$this->columns[$i]])) {
                    $value = $this->columnSchemas[$this->columns[$i]]->dbTypecast($value);
                }
                if (is_string($value)) {
                    $value = $this->schema->quoteValue($value);
                } elseif (is_float($value)) {
                    // ensure type cast always has . as decimal separator in all locales
                    $value = StringHelper::floatToString($value);
                } elseif ($value === false) {
                    $value = 0;
                } elseif ($value === null) {
                    $value = 'NULL';
                }
                $rows[$i] = $value;
            }
        }
        $this->sql .= '(' . implode(', ', $rows) . '),';
        return true;
    }

    /**
     * @return mixed
     */
    public function execute()
    {
        if ($this->hasRows) {
            $this->sql = rtrim($this->sql, ',');
            $this->db->createCommand($this->sql)->execute();
        }
        return $this->hasRows;
    }
}