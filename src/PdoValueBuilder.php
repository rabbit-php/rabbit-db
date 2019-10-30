<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace rabbit\db;

/**
 * Class PdoValueBuilder builds object of the [[PdoValue]] expression class.
 *
 * @author Dmytro Naumenko <d.naumenko.a@gmail.com>
 * @since 2.0.14
 */
class PdoValueBuilder implements ExpressionBuilderInterface
{
    /**
     * {@inheritdoc}
     */
    public function build(ExpressionInterface $expression, array &$params = [])
    {
        $params[count($params)] = $expression;

        return '?';
    }
}
