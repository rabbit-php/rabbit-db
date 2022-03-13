<?php

declare(strict_types=1);

namespace Rabbit\DB;

use Rabbit\Base\Helper\ArrayHelper;

/**
 * Trait QueryTraitExt
 * @package rabbit\db
 */
trait QueryTraitExt
{
    protected ?array $joinWith = null;
    protected ?bool $flag = null;

    public function setFlag(bool $flag): self
    {
        $this->flag = $flag;
        return $this;
    }

    protected function normalizeOrderBy(string|array|ExpressionInterface $columns): array
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

    public function one(): ?array
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

    private function buildWith(array $result): array
    {
        if ($this->joinWith) {
            foreach ($this->joinWith as $key => $with) {
                $on = explode('=', $with[2]);
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

                        $tmp->cache($this->queryCacheDuration, $this->cache);

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

    public function joinWith(array $config, string|bool $on, string $type = 'left join'): self
    {
        if (ArrayHelper::isIndexed($config)) {
            $table = array_shift($config);
            $this->join($type, $table, $on);
            $this->joinWith[$table] = [$type, $table, $on];
        } else {
            foreach ($config as $key => $table) {
                $this->join($type, $table, $on);
                $this->joinWith[$key] = [$type, $table, $on];
                break;
            }
        }
        return $this;
    }

    public function totals(): ?int
    {
        return null;
    }
}
