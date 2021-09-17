<?php
declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Throwable;

class Exception extends \Rabbit\Base\Core\Exception
{
    public array $errorInfo = [];

    public function __construct(string $message, ?array $errorInfo = [], int $code = 0, Throwable $previous = null)
    {
        $this->errorInfo = $errorInfo ?? [];
        parent::__construct($message, $code, $previous);
    }

    public function getName(): string
    {
        return 'Database Exception';
    }

    public function __toString()
    {
        return parent::__toString() . PHP_EOL
            . 'Additional Information:' . PHP_EOL . print_r($this->errorInfo, true);
    }
}
