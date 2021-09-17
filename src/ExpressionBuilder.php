<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

class ExpressionBuilder implements ExpressionBuilderInterface
{
    use ExpressionBuilderTrait;

    public function build(ExpressionInterface $expression, array &$params = []): string
    {
        $params = array_merge($params, $expression->params);
        return $expression->__toString();
    }
}
