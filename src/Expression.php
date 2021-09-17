<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

class Expression implements ExpressionInterface
{
    public ?string $expression;
    public array $params = [];

    public function __construct(string $expression, array $params = [])
    {
        $this->expression = $expression;
        $this->params = $params;
    }

    public function __toString()
    {
        return $this->expression;
    }
}
