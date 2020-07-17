<?php
declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB\Conditions;

use Rabbit\DB\ExpressionBuilderInterface;
use Rabbit\DB\ExpressionBuilderTrait;
use Rabbit\DB\ExpressionInterface;

/**
 * Class LikeConditionBuilder builds objects of [[LikeCondition]]
 *
 * @author Dmytro Naumenko <d.naumenko.a@gmail.com>
 * @since 2.0.14
 */
class LikeConditionBuilder implements ExpressionBuilderInterface
{
    use ExpressionBuilderTrait;

    /**
     * @var array map of chars to their replacements in LIKE conditions.
     * By default it's configured to escape `%`, `_` and `\` with `\`.
     */
    protected array $escapingReplacements = [
        '%' => '\%',
        '_' => '\_',
        '\\' => '\\\\',
    ];
    /**
     * @var string|null character used to escape special characters in LIKE conditions.
     * By default it's assumed to be `\`.
     */
    protected ?string $escapeCharacter = null;


    /**
     * Method builds the raw SQL from the $expression that will not be additionally
     * escaped or quoted.
     *
     * @param ExpressionInterface|LikeCondition $expression the expression to be built.
     * @param array $params the binding parameters.
     * @return string the raw SQL that will not be additionally escaped or quoted.
     */
    public function build(ExpressionInterface $expression, array &$params = []): string
    {
        $operator = $expression->getOperator();
        $column = $expression->getColumn();
        $values = $expression->getValue();
        $escape = $expression->getEscapingReplacements();
        if ($escape === null || $escape === []) {
            $escape = $this->escapingReplacements;
        }

        [$andor, $not, $operator] = $this->parseOperator($operator);

        if (!is_array($values)) {
            $values = [$values];
        }

        if (empty($values)) {
            return $not ? '' : '0=1';
        }

        if (strpos($column, '(') === false) {
            $column = $this->queryBuilder->db->quoteColumnName($column);
        }

        $escapeSql = $this->getEscapeSql();
        $parts = [];
        foreach ($values as $value) {
            if ($value instanceof ExpressionInterface) {
                $phName = $this->queryBuilder->buildExpression($value, $params);
            } else {
                $phName = $this->queryBuilder->bindParam(
                    empty($escape) ? $value : ('%' . strtr($value, $escape) . '%'),
                    $params
                );
            }
            $parts[] = "{$column} {$operator} {$phName}{$escapeSql}";
        }

        return implode($andor, $parts);
    }

    /**
     * @param string $operator
     * @return array
     */
    protected function parseOperator(string $operator): array
    {
        if (!preg_match('/^(AND |OR |)(((NOT |))I?LIKE)/', $operator, $matches)) {
            throw new \InvalidArgumentException("Invalid operator '$operator'.");
        }
        $andor = ' ' . (!empty($matches[1]) ? $matches[1] : 'AND ');
        $not = !empty($matches[3]);
        $operator = $matches[2];

        return [$andor, $not, $operator];
    }

    /**
     * @return string
     */
    private function getEscapeSql(): string
    {
        if ($this->escapeCharacter !== null) {
            return " ESCAPE '{$this->escapeCharacter}'";
        }

        return '';
    }
}
