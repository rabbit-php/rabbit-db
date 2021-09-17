<?php

declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

class IntegrityException extends Exception
{
    public function getName(): string
    {
        return 'Integrity constraint violation';
    }
}
