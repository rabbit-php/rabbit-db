<?php

declare(strict_types=1);

namespace Rabbit\DB;

use Psr\SimpleCache\CacheInterface;
use Rabbit\Base\Helper\ArrayHelper;

/**
 * Class DBHelper
 * @package rabbit\db
 */
class DBHelper
{
    /**
     * @param Query $query
     * @param array $filter
     * @param string $handle
     * @return mixed
     */
    public static function PubSearch(Query $query, array $filter, string $handle)
    {
        $query = static::Search($query, $filter);
        if (is_array($handle)) {
            $k = key($handle);
            $result = $query->$k($handle[key($handle)]);
        } else {
            $result = $query->$handle();
        }
        return $result;
    }

    /**
     * @param Query $query
     * @param array|null $filter
     * @return Query
     */
    public static function Search(Query $query, array $filter = []): Query
    {
        foreach ($filter as $method => $value) {
            switch ($method) {
                case (strpos(strtolower($method), 'where') !== false || in_array(strtolower($method), ['select', 'from'])) && is_array($value):
                    foreach ($value as $key => $data) {
                        if (is_array($data)) {
                            if (isset($data['query'])) {
                                $value[$key] = self::Search(new Query(), $data['query']);
                            } elseif (isset($data['exp'])) {
                                if (is_string($data['exp']) && strpos($data['exp'], ';') !== false) {
                                    throw new Exception("Sql can not include ';'!");
                                }
                                $value[$key] = new Expression(...(array)$data['exp']);
                            }
                        }
                    }
                    break;
                case str_ends_with($method, '[]'):
                    $method = substr($method, -1, 2);
                    foreach ($value as $data) {
                        self::Search($query, [$method => $data]);
                    }
                    continue 2;
            }
            if (is_string($value) && strpos($value, ';') !== false) {
                throw new Exception("Sql can not include ';'!");
            }
            $query->$method($value);
        }
        return $query;
    }

    /**
     * @param Query $query
     * @param array|null $filter
     * @param int $page
     * @param int $duration
     * @param CacheInterface|null $cache
     * @return array
     */
    public static function SearchList(
        Query $query,
        array $filter = [],
        int $page = 0,
        int $duration = -1,
        ?CacheInterface $cache = null
    ): array {
        $limit = ArrayHelper::remove($filter, 'limit', 20);
        $offset = ArrayHelper::remove($filter, 'offset', ($page ? ($page - 1) : 0) * (int)$limit);
        $count = ArrayHelper::remove($filter, 'count', '1');
        $queryRes = $filter === [] || !$filter ? $query : static::Search($query, $filter);
        $rows = $queryRes->cache($duration, $cache)->limit($limit ?: null)->offset($offset)->all();
        if ($limit) {
            $query->limit = null;
            $query->offset = null;
            $total = $queryRes->cache($duration, $cache)->count($count);
        } else {
            $total = count($rows);
        }
        return ['total' => $total, 'data' => $rows];
    }
}
