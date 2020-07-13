<?php
declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB\Conditions;

use rabbit\db\Query;

/**
 * Condition that represents `EXISTS` operator.
 *
 * @author Dmytro Naumenko <d.naumenko.a@gmail.com>
 * @since 2.0.14
 */
class ExistsCondition implements ConditionInterface
{
    /**
     * @var string $operator the operator to use (e.g. `EXISTS` or `NOT EXISTS`)
     */
    private string $operator;
    /**
     * @var Query the [[Query]] object representing the sub-query.
     */
    private Query $query;


    /**
     * ExistsCondition constructor.
     *
     * @param string $operator the operator to use (e.g. `EXISTS` or `NOT EXISTS`)
     * @param Query $query the [[Query]] object representing the sub-query.
     */
    public function __construct(string $operator, Query $query)
    {
        $this->operator = $operator;
        $this->query = $query;
    }

    /**
     * {@inheritdoc}
     */
    public static function fromArrayDefinition(string $operator, array $operands): self
    {
        if (!isset($operands[0]) || !$operands[0] instanceof Query) {
            throw new \InvalidArgumentException('Subquery for EXISTS operator must be a Query object.');
        }

        return new static($operator, $operands[0]);
    }

    /**
     * @return string
     */
    public function getOperator(): string
    {
        return $this->operator;
    }

    /**
     * @return Query
     */
    public function getQuery(): Query
    {
        return $this->query;
    }
}
