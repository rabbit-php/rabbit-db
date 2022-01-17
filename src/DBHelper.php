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

    public static function Search(Query $query, array $filter = []): Query
    {
        foreach ($filter as $method => $value) {
            if (str_ends_with($method, '{}')) {
                $method = str_replace('{}', '', $method);
                foreach ($value as $data) {
                    self::Search($query, [$method => $data]);
                }
                continue;
            }
            if ((str_contains(strtolower($method), 'where') || in_array(strtolower($method), ['select', 'from'])) && is_array($value)) {
                foreach ($value as $key => $data) {
                    if (is_array($data)) {
                        if (isset($data['query'])) {
                            $value[$key] = self::Search(new Query(), $data['query']);
                        } elseif (isset($data['exp'])) {
                            if (is_string($data['exp']) && str_contains($data['exp'], ';') !== false) {
                                throw new Exception("Sql can not include ';'!");
                            }
                            $value[$key] = new Expression(...(array)$data['exp']);
                        }
                    }
                }
            }
            if (is_string($value) && str_contains($value, ';') !== false) {
                throw new Exception("Sql can not include ';'!");
            }
            if (!empty($value)) {
                if (str_starts_with($method, '&')) {
                    $query->setFlag(true);
                    $method = str_replace('&', '', $method);
                }
                if (str_ends_with($method, '>')) {
                    $method = str_replace('>', '', $method);
                    $query->$method(...$value);
                } else {
                    $query->$method($value);
                }
            }
        }
        return $query;
    }

    public static function SearchList(
        Query $query,
        array $filter = [],
        int $page = 0,
        int $duration = -1,
        ?CacheInterface $cache = null,
        string $totalKey = 'total',
        string $listKey = 'data'
    ): array {
        $limit = ArrayHelper::remove($filter, 'limit', 20);
        $offset = ArrayHelper::remove($filter, 'offset', ($page ? ($page - 1) : 0) * (int)$limit);
        $count = ArrayHelper::remove($filter, 'count', '1');
        $queryRes = $filter === [] || !$filter ? $query : static::Search($query, $filter);
        $rows = $queryRes->cache($duration, $cache)->limit($limit ?: null)->offset($offset)->all();
        if ($limit) {
            if (null === $total = $queryRes->totals()) {
                $query->limit = null;
                $query->offset = null;
                $total = $queryRes->cache($duration, $cache)->count($count);
            }
        } else {
            $total = count($rows);
        }
        return [$totalKey => $total, $listKey => $rows];
    }
}
