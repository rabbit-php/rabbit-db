<?php

namespace rabbit\db;

use rabbit\helper\ArrayHelper;

/**
 * Class DBHelper
 * @package rabbit\db
 */
class DBHelper
{
    /**
     * @param Query $query
     * @param array $filter
     * @param $handle
     * @param $db
     * @return mixed
     */
    public static function PubSearch(Query $query, array $filter, $handle, $db)
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
    public static function Search(Query $query, array $filter = null): Query
    {
        if (!empty($filter)) {
            foreach ($filter as $k => $v) {
                $query->$k($v);
            }
        }
        return $query;
    }

    /**
     * @param Query $query
     * @param array|null $filter
     * @param int $page
     * @return array
     */
    public static function SearchList(Query $query, array $filter = [], int $page = 0): array
    {
        $limit = ArrayHelper::remove($filter, 'limit', 20);
        $offset = ArrayHelper::remove($filter, 'offset', $page ? ($page - 1) : 0 * (int)$limit);
        $count = ArrayHelper::remove($filter, 'count', '1');

        $queryRes = $filter === [] || !$filter ? $query : static::Search($query, $filter);

        if ($query instanceof \rabbit\activerecord\ActiveQuery) {
            $rows = $queryRes->limit($limit ?: null)->offset($offset)->asArray()->all();
        } else {
            $rows = $queryRes->limit($limit ?: null)->offset($offset)->all();
        }
        if ($limit) {
            $query->limit = null;
            $query->offset = null;
            $total = $queryRes->count($count);
        } else {
            $total = count($rows);
        }
        unset($query);
        unset($queryRes);
        return array(['total' => $total, 'data' => $rows]);
    }

}
