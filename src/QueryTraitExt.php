<?php

declare(strict_types=1);

namespace Rabbit\DB;

use Psr\SimpleCache\InvalidArgumentException;
use Throwable;

/**
 * Trait QueryTraitExt
 * @package rabbit\db
 */
trait QueryTraitExt
{
    /** @var array */
    protected ?array $joinWith = null;
    /** @var bool */
    protected bool $flag = false;

    /**
     * @param $columns
     * @return array|Expression[]
     */
    protected function normalizeOrderBy($columns)
    {
        if ($columns instanceof Expression) {
            return [$columns];
        } elseif (is_array($columns)) {
            foreach ($columns as $key => $sort) {
                if (is_string($sort)) {
                    if (strtolower($sort) === 'desc') {
                        $columns[$key] = SORT_DESC;
                    } elseif (strtolower($sort) === 'asc') {
                        $columns[$key] = SORT_ASC;
                    }
                }
            }
            return $columns;
        } else {
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
            $result = [];
            foreach ($columns as $column) {
                if (preg_match('/^(.*?)\s+(asc|desc)$/i', $column, $matches)) {
                    $result[$matches[1]] = strcasecmp($matches[2], 'desc') ? SORT_ASC : SORT_DESC;
                } else {
                    $result[$column] = SORT_ASC;
                }
            }
            return $result;
        }
    }

    /**
     * @param array $rows
     * @return array
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function populate(array $rows): array
    {
        $rows = $this->buildWith($rows);
        if ($this->indexBy === null) {
            return $rows;
        }

        $result = [];
        foreach ($rows as $row) {
            if (is_string($this->indexBy)) {
                $key = $row[$this->indexBy];
            } else {
                $key = call_user_func($this->indexBy, $row);
            }
            $result[$key] = $row;
        }
        return $this->buildWith($result);
    }

    /**
     * @return mixed|null
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function one()
    {
        if ($this->emulateExecution) {
            return null;
        }
        $this->limit(1);
        $result = $this->createCommand()->queryOne();
        if ($result) {
            $list[] = $result;
            $result = current($this->buildWith($list));
        }
        return $result;
    }

    /**
     * @param array $result
     * @return array
     * @throws InvalidArgumentException
     * @throws Throwable
     */
    public function buildWith(array $result): array
    {
        if (is_array($this->joinWith)) {
            foreach ($this->joinWith as $key => $with) {
                $on = $with[2];
                $on = explode('=', $on);
                $lfield = explode('.', $on[0]);
                $lfield = count($lfield) == 1 ? $lfield[0] : $lfield[1];
                foreach ($result as $row) {
                    $ids[] = $row[$lfield];
                }

                foreach ($this->join as $join) {
                    if ($with === $join) {
                        $field = explode('.', $on[1]);
                        $field = count($field) == 1 ? $field[0] : $field[1];
                        if (is_string($join[1])) {
                            $tmp = (new Query($this->db))->from((array)$join[1])->where([$field => $ids]);
                        } else if (is_array($join[1])) {
                            $query = $join[1][key($join[1])];
                            $tmp = $query->where([$field => $ids]);
                        }

                        $res = $this->flag ? $tmp->all() : [$tmp->one()];

                        foreach ($res as $t) {
                            foreach ($result as $k => $r) {
                                if ($t[$field] === $r[$lfield]) {
                                    if ($this->flag) {
                                        $result[$k][$key][] = $t;
                                    } else {
                                        $result[$k][$key] = $t;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
        return $result;
    }

    /**
     * @param array $list
     * @return QueryTraitExt
     */
    public function joinWithOne(array $list): self
    {
        $this->flag = false;
        return $this->group($list);
    }

    /**
     * @param array $list
     * @return QueryTraitExt
     */
    public function joinWithMany(array $list): self
    {
        $this->flag = true;
        return $this->group($list);
    }

    /**
     * @param array $list
     * @return $this
     */
    public function group(array $list): self
    {
        foreach ($list as $key => $join) {
            if (is_array($join) && ($num = count($join)) > 1) {
                [$table, $on] = $join;
                if ($num > 2) {
                    $type = $join[2];
                } else {
                    $type = 'left join';
                }
                if ($num > 3) {
                    $addOn = $join[3];
                } else {
                    $addOn = [];
                }

                foreach ($addOn as $val) {
                    $on = [key($val), $on, $val[key($val)]];
                }
                $this->join($type, $table, $on);
                $this->joinWith[$key] = [$type, $table, $on];
            }
        }
        return $this;
    }
}
