<?php
declare(strict_types=1);

namespace rabbit\db;

/**
 * Trait QueryTraitExt
 * @package rabbit\db
 */
trait QueryTraitExt
{
    /**
     * @param array $list
     * @return $this
     */
    public function joinList(array $list): self
    {
        if (is_array($list)) {
            foreach ($list as $join) {
                if (is_array($join)) {
                    $type = "left join";
                    list($table, $on) = $this->getJoinList($join);
                    $this->join($type, $table, $on);
                }
            }
        }
        return $this;
    }

    /**
     * @param array $list
     * @return $this
     */
    public function innerJoinList(array $list): self
    {
        foreach ($list as $join) {
            if (is_array($join)) {
                list($table, $on) = $this->getJoinList($join);
                $this->innerJoin($table, $on);
            }
        }
        return $this;
    }

    /**
     * @param array $list
     * @return $this
     */
    public function leftJoinList(array $list): self
    {
        foreach ($list as $join) {
            if (is_array($join)) {
                list($table, $on) = $this->getJoinList($join);
                $this->leftJoin($table, $on);
            }
        }
        return $this;
    }

    /**
     * @param array $list
     * @return $this
     */
    public function rightJoinList(array $list): self
    {
        foreach ($list as $join) {
            if (is_array($join)) {
                list($table, $on) = $this->getJoinList($join);
                $this->rightJoin($table, $on);
            }
        }
        return $this;
    }

    /**
     * @param $join
     * @return array
     */
    private function getJoinList(array $join): array
    {
        $on = '';
        if (array_key_exists('table', $join)) {
            $table = $join['table'];
        }
        if (array_key_exists('on', $join)) {
            $on = $join['on'];
        }
        if (array_key_exists('addOn', $join)) {
            if (is_array($join['addOn'])) {
                foreach ($join['addOn'] as $val) {
                    $on = [key($val), $on, $val[key($val)]];
                }
            }
        }
        if (array_key_exists('addOnFilter', $join)) {
            if (is_array($join['addOnFilter'])) {
                foreach ($join['addOnFilter'] as $val) {
                    $fon = [];
                    foreach ($val[key($val)] as $fkey => $fval) {
                        if ($fval) {
                            $fon[$fkey] = $fval;
                        }
                    }
                    $on = [key($val), $on, $fon];
                }
            }
        }
        if (empty($table) && empty($on)) {
            [$table, $on] = $join;
        }
        return [$table, $on];
    }
}