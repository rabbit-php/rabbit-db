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
     * @param ConnectionInterface $db
     * @return mixed
     */
    public static function PubSearch(Query $query, array $filter, string $handle, ConnectionInterface $db = null)
    {
        $query = static::Search($query, $filter);
        if (is_array($handle)) {
            $k = key($handle);
            $result = $query->$k($handle[key($handle)], $db);
        } else {
            $result = $query->$handle($db);
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
     * @param ConnectionInterface|null $db
     * @return array
     */
    public static function SearchList(
        Query $query,
        array $filter = [],
        int $page = 0,
        int $duration = -1,
        ?CacheInterface $cache = null,
        ConnectionInterface $db = null
    ): array
    {
        $limit = ArrayHelper::remove($filter, 'limit', 20);
        $offset = ArrayHelper::remove($filter, 'offset', ($page ? ($page - 1) : 0) * (int)$limit);
        $count = ArrayHelper::remove($filter, 'count', '1');
        $queryRes = $filter === [] || !$filter ? $query : static::Search($query, $filter);
        $rows = $queryRes->cache($duration, $cache)->limit($limit ?: null)->offset($offset)->all($db);
        if ($limit) {
            $query->limit = null;
            $query->offset = null;
            $total = $queryRes->cache($duration, $cache)->count($count, $db);
        } else {
            $total = count($rows);
        }
        return ['total' => $total, 'data' => $rows];
    }
}
