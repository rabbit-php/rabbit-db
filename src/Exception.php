<?php
declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB;

use Throwable;

/**
 * Exception represents an exception that is caused by some DB-related operations.
 *
 * @author Qiang Xue <qiang.xue@gmail.com>
 * @since 2.0
 */
class Exception extends \Rabbit\Base\Core\Exception
{
    /**
     * @var array the error info provided by a PDO exception. This is the same as returned
     * by [PDO::errorInfo](http://www.php.net/manual/en/pdo.errorinfo.php).
     */
    public array $errorInfo = [];


    /**
     * Constructor.
     * @param string $message PDO error message
     * @param array $errorInfo PDO error info
     * @param int $code PDO error code
     * @param Throwable $previous The previous exception used for the exception chaining.
     */
    public function __construct(string $message, ?array $errorInfo = [], int $code = 0, Throwable $previous = null)
    {
        $this->errorInfo = $errorInfo ?? [];
        parent::__construct($message, $code, $previous);
    }

    /**
     * @return string the user-friendly name of this exception
     */
    public function getName(): string
    {
        return 'Database Exception';
    }

    /**
     * @return string readable representation of exception
     */
    public function __toString()
    {
        return parent::__toString() . PHP_EOL
            . 'Additional Information:' . PHP_EOL . print_r($this->errorInfo, true);
    }
}
