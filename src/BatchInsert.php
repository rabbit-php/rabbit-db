<?php


namespace rabbit\db;

use rabbit\helper\StringHelper;

/**'
 * Class BatchInsert
 * @package rabbit\db
 */
class BatchInsert
{
    /** @var array */
    private $columnSchemas = [];
    /** @var */
    private $sql;
    /** @var ConnectionInterface */
    private $db;

    /**
     * BatchInsert constructor.
     * @param string $table
     * @param array $columns
     * @param ConnectionInterface $db
     */
    public function __construct(string $table, ConnectionInterface $db)
    {
        $this->db = $db;


        $this->sql = 'INSERT INTO ' . $schema->quoteTableName($table);
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
        $schema = $this->db->getSchema();
        if (($tableSchema = $schema->getTableSchema($table)) !== null) {
            $this->columnSchemas = $tableSchema->columns;
        }

        foreach ($columns as $i => $name) {
            $columns[$i] = $schema->quoteColumnName($name);
        }
        $this->sql .= ' (' . implode(', ', $columns) . ') VALUES ';
        return true;
    }

    /**
     * @param array $row
     * @param bool $checkFields
     * @return bool
     */
    public function addRow(array $row, bool $checkFields = true): bool
    {
        if (empty($rows)) {
            return false;
        }
        if ($checkFields) {
            $schema = $this->db->getSchema();
            foreach ($row as $i => $value) {
                if (isset($columns[$i], $this->columnSchemas[$columns[$i]])) {
                    $value = $this->columnSchemas[$columns[$i]]->dbTypecast($value);
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
                }
                $row[$i] = $value;
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
        $this->sql = rtrim(',', $this->sql);
        return $this->db->createCommand($this->sql)->execute();
    }
}