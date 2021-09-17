<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

final class PdoValue implements ExpressionInterface
{
    private null|string|array|float|int $value;

    private int $type;

    public function __construct(null|string|array|float|int $value, int $type)
    {
        $this->value = $value;
        $this->type = $type;
    }

    public function getValue(): null|string|array|float|int
    {
        return $this->value;
    }

    public function getType(): int
    {
        return $this->type;
    }
}
