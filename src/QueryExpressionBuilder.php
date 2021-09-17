<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

class QueryExpressionBuilder implements ExpressionBuilderInterface
{
    use ExpressionBuilderTrait;

    public function build(ExpressionInterface $expression, array &$params = []): string
    {
        [$sql, $params] = $this->queryBuilder->build($expression, $params);

        return "($sql)";
    }
}
