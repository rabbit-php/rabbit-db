<?php
declare(strict_types=1);
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace Rabbit\DB\Conditions;

/**
 * Condition based on column-value pairs.
 *
 * @author Dmytro Naumenko <d.naumenko.a@gmail.com>
 * @since 2.0.14
 */
class HashCondition implements ConditionInterface
{
    /**
     * @var array|null the condition specification.
     */
    private ?array $hash;


    /**
     * HashCondition constructor.
     *
     * @param array|null $hash
     */
    public function __construct(?array $hash)
    {
        $this->hash = $hash;
    }

    /**
     * {@inheritdoc}
     */
    public static function fromArrayDefinition(string $operator, ?array $operands): self
    {
        return new static($operands);
    }

    /**
     * @return array|null
     */
    public function getHash(): ?array
    {
        return $this->hash;
    }
}
