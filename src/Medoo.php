<?php

declare(strict_types=1);

namespace maihuoche;

class Medoo
{
    /**
     * Database connection options
     */
    protected array $options = [];

    /**
     * PDO instance
     */
    protected ?\PDO $pdo = null;

    /**
     * Database query statement
     */
    protected ?\PDOStatement $statement = null;

    /**
     * Last query SQL string
     */
    protected ?string $debugQuery = null;

    /**
     * The parameters for last query
     */
    protected array $debugParams = [];

    /**
     * Current table prefix
     */
    protected string $prefix = '';

    /**
     * Current database type
     */
    protected string $type = 'mysql';

    /**
     * Initialize the Medoo object
     * 
     * @throws \InvalidArgumentException
     */
    public function __construct(array $options)
    {
        if (empty($options['database'])) {
            throw new \InvalidArgumentException("Database name is required");
        }
        
        if (empty($options['host']) && empty($options['socket'])) {
            throw new \InvalidArgumentException("Either host or socket must be specified");
        }

        if (!isset($options['username'])) {
            throw new \InvalidArgumentException("Username is required");
        }
        
        $this->options = $options;
        $this->prefix = $options['prefix'] ?? '';
        $this->connect();
    }

    /**
     * Create a database connection
     */
    protected function connect(): void
    {
        $options = $this->options;
        
        // Charset and collation
        $charset = $options['charset'] ?? 'utf8mb4';
        $collation = $options['collation'] ?? 'utf8mb4_general_ci';
        
        // DSN construction
        $dsn_parts = [];
        $dsn_parts[] = 'charset=' . $charset;
        
        if (!empty($options['socket'])) {
            $dsn_parts[] = 'unix_socket=' . $options['socket'];
        } else {
            // Host and port
            $dsn_parts[] = 'host=' . $options['host'];
            
            if (!empty($options['port'])) {
                $dsn_parts[] = 'port=' . $options['port'];
            }
        }
        
        $dsn_parts[] = 'dbname=' . $options['database'];
        
        // PDO options
        $driver_options = [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            \PDO::ATTR_EMULATE_PREPARES => false,
            \PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES ' . $charset . 
                ' COLLATE ' . $collation
        ];
        
        // Merge with custom PDO options if provided
        if (isset($options['pdo_options']) && is_array($options['pdo_options'])) {
            $driver_options = array_replace($driver_options, $options['pdo_options']);
        }
        
        // Create PDO instance
        $this->pdo = new \PDO(
            'mysql:' . implode(';', $dsn_parts),
            $options['username'] ?? '',
            $options['password'] ?? '',
            $driver_options
        );
    }

    /**
     * Get the PDO instance
     */
    public function pdo(): ?\PDO
    {
        return $this->pdo;
    }

    /**
     * Begin a transaction
     * 
     * @throws \RuntimeException
     */
    public function beginTransaction(): bool
    {
        if ($this->pdo->inTransaction()) {
            throw new \RuntimeException("Transaction already started");
        }
        return $this->pdo->beginTransaction();
    }

    /**
     * Commit a transaction
     * 
     * @throws \RuntimeException
     */
    public function commit(): bool
    {
        if (!$this->pdo->inTransaction()) {
            throw new \RuntimeException("No active transaction to commit");
        }
        return $this->pdo->commit();
    }

    /**
     * Rollback a transaction
     * 
     * @throws \RuntimeException
     */
    public function rollBack(): bool
    {
        if (!$this->pdo->inTransaction()) {
            throw new \RuntimeException("No active transaction to rollback");
        }
        return $this->pdo->rollBack();
    }

    /**
     * Execute raw SQL query
     * 
     * @return \PDOStatement|bool PDOStatement on success, false on failure
     */
    public function query(string $query, array $params = []): \PDOStatement|bool
    {
        $this->statement = $this->pdo->prepare($query);
        $this->debugQuery = $query;
        $this->debugParams = $params;
        
        return $this->statement->execute($params);
    }

    /**
     * Get the last query SQL and parameters
     */
    public function debug(): array
    {
        return [
            'query' => $this->debugQuery,
            'params' => $this->debugParams
        ];
    }

    /**
     * Select data from database
     * 
     * @param string|array $table
     * @param array $columns
     * @param array|null $where
     * @param array|null $join
     * @return array
     * @throws \InvalidArgumentException
     */
    public function select(string|array $table, array $columns = ['*'], ?array $where = null, ?array $join = null): array
    {
        if (empty($columns)) {
            throw new \InvalidArgumentException("Columns array cannot be empty");
        }

        $map = [];
        $table_query = $this->buildTable($table);
        
        $column_query = $this->buildColumns($columns, isset($join));
        
        $join_query = '';
        if ($join && is_array($join)) {
            $join_query = $this->buildJoin($join, $map);
        }
        
        $where_clause = '';
        if ($where !== null) {
            $where_result = $this->buildWhere($where, $map);
            $where_clause = $where_result['where'] ? ' WHERE ' . $where_result['where'] : '';
            $map = $where_result['map'];
        }
        
        $query = "SELECT {$column_query} FROM {$table_query}{$join_query}{$where_clause}";
        
        $this->statement = $this->pdo->prepare($query);
        $this->debugQuery = $query;
        $this->debugParams = $map;
        
        $this->statement->execute($map);
        
        return $this->statement->fetchAll();
    }

    /**
     * Insert data into database
     * 
     * @param string $table
     * @param array $data
     * @return string
     * @throws \InvalidArgumentException
     * @throws \RuntimeException
     */
    public function insert(string $table, array $data): string
    {
        if (empty($data)) {
            throw new \InvalidArgumentException("Data array cannot be empty");
        }

        $keys = array_keys($data);
        $columns = implode(', ', array_map(fn($key) => "`{$key}`", $keys));
        $placeholders = implode(', ', array_fill(0, count($keys), '?'));
        
        $query = "INSERT INTO {$this->buildTable($table)} ({$columns}) VALUES ({$placeholders})";
        
        $this->statement = $this->pdo->prepare($query);
        $this->debugQuery = $query;
        $this->debugParams = array_values($data);
        
        if (!$this->statement->execute(array_values($data))) {
            throw new \RuntimeException("Failed to insert data");
        }
        
        return $this->pdo->lastInsertId();
    }

    /**
     * Update data in database
     * 
     * @param string $table
     * @param array $data
     * @param array|null $where
     * @return int
     * @throws \InvalidArgumentException
     */
    public function update(string $table, array $data, ?array $where = null): int
    {
        if (empty($data)) {
            throw new \InvalidArgumentException("Data array cannot be empty");
        }

        $map = [];
        $table_query = $this->buildTable($table);
        
        $set_query = $this->buildSet($data, $map);
        
        $where_clause = '';
        if ($where !== null) {
            $where_result = $this->buildWhere($where, $map);
            $where_clause = $where_result['where'] ? ' WHERE ' . $where_result['where'] : '';
            $map = array_merge($map, $where_result['map']);
        }
        
        $query = "UPDATE {$table_query} SET {$set_query}{$where_clause}";
        
        $this->statement = $this->pdo->prepare($query);
        $this->debugQuery = $query;
        $this->debugParams = $map;
        
        $this->statement->execute($map);
        
        return $this->statement->rowCount();
    }

    /**
     * Delete data from database
     * 
     * @param string $table
     * @param array|null $where
     * @return int Number of affected rows
     */
    public function delete(string $table, ?array $where = null): int
    {
        $map = [];
        $table_query = $this->buildTable($table);
        
        $where_clause = '';
        if ($where !== null) {
            $where_result = $this->buildWhere($where, $map);
            $where_clause = $where_result['where'] ? ' WHERE ' . $where_result['where'] : '';
            $map = $where_result['map'];
        }
        
        $query = "DELETE FROM {$table_query}{$where_clause}";
        
        $this->statement = $this->pdo->prepare($query);
        $this->debugQuery = $query;
        $this->debugParams = $map;
        
        $this->statement->execute($map);
        
        return $this->statement->rowCount();
    }

    /**
     * Build the table name with prefix
     */
    protected function buildTable(string|array $table): string
    {
        if (is_array($table)) {
            $table_array = [];
            $alias_array = [];
            
            foreach ($table as $key => $value) {
                if (is_string($key)) {
                    $table_array[] = $this->tableQuote($value);
                    $alias_array[] = $this->tableQuote($key);
                } else {
                    $table_array[] = $this->tableQuote($value);
                }
            }
            
            return implode(', ', $table_array) . 
                   (count($alias_array) > 0 ? ' AS ' . implode(', ', $alias_array) : '');
        }
        
        return $this->tableQuote($table);
    }

    /**
     * Build the JOIN clauses
     */
    protected function buildJoin(array $join, array &$map): string
    {
        $join_array = [];
        
        foreach ($join as $table => $relation) {
            $type = 'INNER';
            $table_name = '';
            
            if (is_array($relation)) {
                if (isset($relation[0])) {
                    $type = strtoupper($relation[0]);
                    
                    if (!in_array($type, ['LEFT', 'RIGHT', 'FULL', 'INNER', 'CROSS'])) {
                        $type = 'INNER';
                    }
                }
                
                $table_name = $relation[1] ?? '';
            } elseif (is_string($relation)) {
                $table_name = $relation;
            }
            
            $table_query = $this->buildTable($table_name);
            
            $on_condition = '';
            if (isset($relation[2]) && !empty($relation[2])) {
                $on_result = $this->buildJoinCondition($relation[2], $map);
                $on_condition = ' ON ' . $on_result;
            }
            
            $join_array[] = "{$type} JOIN {$table_query}{$on_condition}";
        }
        
        return ' ' . implode(' ', $join_array);
    }

    /**
     * Build JOIN condition
     */
    protected function buildJoinCondition(array $conditions, array &$map): string
    {
        $join_conditions = [];
        
        foreach ($conditions as $key => $value) {
            if ($key === 'AND' || $key === 'OR') {
                $join_conditions[] = '(' . $this->buildJoinCondition($value, $map) . ')';
            } else {
                // Handle raw comparison (table1.column = table2.column)
                if (strpos($key, '.') !== false && strpos($value, '.') !== false) {
                    $join_conditions[] = $this->columnQuote($key) . ' = ' . $this->columnQuote($value);
                }
                // Handle column-value comparison
                else {
                    $join_conditions[] = $this->buildCondition($key, $value, $map);
                }
            }
        }
        
        return implode(' AND ', $join_conditions);
    }

    /**
     * Build the columns part of the SQL query
     * 
     * @throws \InvalidArgumentException
     */
    protected function buildColumns(array $columns, bool $isJoin = false): string
    {
        if (empty($columns)) {
            throw new \InvalidArgumentException("Columns array cannot be empty");
        }

        if (in_array('*', $columns) && !$isJoin) {
            return '*';
        }
        
        $result = [];
        
        foreach ($columns as $key => $value) {
            if (is_string($key)) {
                $result[] = $this->columnQuote($value) . ' AS ' . $this->columnQuote($key);
            } elseif ($isJoin && strpos($value, '.') !== false) {
                $result[] = $this->columnQuote($value);
            } else {
                $result[] = $this->columnQuote($value);
            }
        }
        
        return implode(', ', $result);
    }

    /**
     * Build the SET clause for UPDATE queries
     */
    protected function buildSet(array $data, array &$map): string
    {
        $set = [];
        
        foreach ($data as $column => $value) {
            $column = $this->columnQuote($column);
            $map[] = $value;
            $set[] = "{$column} = ?";
        }
        
        return implode(', ', $set);
    }

    /**
     * Build the WHERE clause
     */
    protected function buildWhere(array $where, array &$map): array
    {
        $whereGroups = [];
        
        foreach ($where as $column => $value) {
            if ($column === 'AND' || $column === 'OR') {
                $whereGroups[] = '(' . $this->buildGroupedWhere($value, $map, $column) . ')';
            } else {
                $whereGroups[] = $this->buildCondition($column, $value, $map);
            }
        }
        
        return [
            'where' => implode(' AND ', $whereGroups),
            'map' => $map
        ];
    }

    /**
     * Build grouped WHERE conditions
     */
    protected function buildGroupedWhere(array $conditions, array &$map, string $operator): string
    {
        $groups = [];
        
        foreach ($conditions as $condition) {
            $result = $this->buildWhere($condition, $map);
            if ($result['where']) {
                $groups[] = $result['where'];
            }
        }
        
        return implode(" {$operator} ", $groups);
    }

    /**
     * Build a single condition for WHERE clause
     */
    protected function buildCondition(string $column, mixed $value, array &$map): string
    {
        $column = $this->columnQuote($column);
        
        if (is_null($value)) {
            return "{$column} IS NULL";
        }
        
        if (is_array($value)) {
            if (isset($value[0]) && is_string($value[0]) && strtoupper($value[0]) === 'NOT NULL') {
                return "{$column} IS NOT NULL";
            }
            
            if (isset($value[0]) && in_array(strtoupper($value[0]), ['IN', 'NOT IN'])) {
                $operator = strtoupper($value[0]);
                $values = array_slice($value, 1);
                
                $placeholders = [];
                foreach ($values as $item) {
                    $map[] = $item;
                    $placeholders[] = '?';
                }
                
                $placeholders_str = implode(', ', $placeholders);
                return "{$column} {$operator} ({$placeholders_str})";
            }
            
            if (isset($value[0]) && in_array(strtoupper($value[0]), ['BETWEEN', 'NOT BETWEEN'])) {
                $operator = strtoupper($value[0]);
                $map[] = $value[1];
                $map[] = $value[2];
                
                return "{$column} {$operator} ? AND ?";
            }
        }
        
        // Default: equals comparison
        $map[] = $value;
        return "{$column} = ?";
    }

    /**
     * Quote a table name
     */
    protected function tableQuote(string $table): string
    {
        return '`' . $this->prefix . str_replace('`', '``', $table) . '`';
    }

    /**
     * Quote a column name
     */
    protected function columnQuote(string $column): string
    {
        if (strpos($column, '.') !== false) {
            $parts = explode('.', $column);
            return '`' . str_replace('`', '``', $parts[0]) . 
                   '`.`' . str_replace('`', '``', $parts[1]) . '`';
        }
        
        return '`' . str_replace('`', '``', $column) . '`';
    }
}
