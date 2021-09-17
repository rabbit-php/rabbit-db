<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

class PdoValueBuilder implements ExpressionBuilderInterface
{
    public function build(ExpressionInterface $expression, array &$params = []): string
    {
        $params[count($params)] = $expression;

        return '?';
    }
}
