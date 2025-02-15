# 精简版Medoo使用教程

这篇教程将详细介绍如何使用我们的精简版Medoo库，它是一个轻量级、现代化的MySQL数据库操作框架，专为PHP 8.3及以上版本优化。

## 目录
1. [安装与配置](#安装与配置)
2. [建立连接](#建立连接)
3. [基本查询操作](#基本查询操作)
    - [SELECT查询](#select查询)
    - [INSERT插入](#insert插入)
    - [UPDATE更新](#update更新)
    - [DELETE删除](#delete删除)
4. [JOIN连接查询](#join连接查询)
5. [条件查询](#条件查询)
6. [事务处理](#事务处理)
7. [调试与错误处理](#调试与错误处理)
8. [高级用法](#高级用法)
9. [最佳实践](#最佳实践)

## 安装与配置

### 通过Composer安装

```bash
composer require maihuoche/lite-medoo
```

### 手动安装

将`Medoo.php`文件复制到你的项目中，确保正确设置命名空间。

```php
<?php
// 引入Medoo类
require_once 'path/to/Medoo.php';

use Medoo\Medoo;
```

## 建立连接

创建数据库连接是使用Medoo的第一步。

```php
// 基本连接
$database = new Medoo([
    'host' => 'localhost',
    'database' => 'my_database',
    'username' => 'root',
    'password' => 'password'
]);

// 高级配置
$database = new Medoo([
    'host' => 'localhost',
    'port' => 3306,
    'database' => 'my_database',
    'username' => 'root',
    'password' => 'password',
    'charset' => 'utf8mb4',
    'collation' => 'utf8mb4_unicode_ci',
    'prefix' => 'prefix_',
    'pdo_options' => [
        PDO::ATTR_CASE => PDO::CASE_NATURAL,
        PDO::ATTR_TIMEOUT => 5
    ]
]);

// 使用Unix套接字连接
$database = new Medoo([
    'socket' => '/tmp/mysql.sock',
    'database' => 'my_database',
    'username' => 'root',
    'password' => 'password'
]);
```

### 连接参数说明

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| host | string | 是(与socket二选一) | 数据库主机地址 |
| socket | string | 是(与host二选一) | Unix套接字路径 |
| port | int | 否 | 数据库端口，默认3306 |
| database | string | 是 | 数据库名称 |
| username | string | 否 | 数据库用户名 |
| password | string | 否 | 数据库密码 |
| charset | string | 否 | 字符集，默认utf8mb4 |
| collation | string | 否 | 校对规则，默认utf8mb4_general_ci |
| prefix | string | 否 | 表前缀 |
| pdo_options | array | 否 | 自定义PDO选项 |

## 基本查询操作

### SELECT查询

从数据库中检索数据。

```php
// 查询所有列
$data = $database->select('users', '*');

// 查询指定列
$data = $database->select('users', ['id', 'name', 'email']);

// 带条件的查询
$data = $database->select('users', ['id', 'name', 'email'], [
    'status' => 'active',
    'age[>]' => 18
]);

// 给列起别名
$data = $database->select('users', [
    'user_id' => 'id',
    'full_name' => 'name'
]);

// 查询指定表的所有记录的指定字段
$allUsers = $database->select('users', ['id', 'name', 'email']);
foreach ($allUsers as $user) {
    echo "ID: {$user['id']}, Name: {$user['name']}, Email: {$user['email']}\n";
}
```

### INSERT插入

向数据库中插入数据。

```php
// 插入单条记录
$lastId = $database->insert('users', [
    'name' => 'John Doe',
    'email' => 'john@example.com',
    'created_at' => date('Y-m-d H:i:s')
]);

if ($lastId) {
    echo "新插入的用户ID: {$lastId}";
} else {
    echo "插入失败";
}
```

### UPDATE更新

更新数据库中的数据。

```php
// 更新单条记录
$affected = $database->update('users', 
    [
        'name' => 'Jane Doe',
        'updated_at' => date('Y-m-d H:i:s')
    ],
    [
        'id' => 10
    ]
);

echo "受影响的行数: {$affected}";

// 更新多条记录
$affected = $database->update('users',
    ['status' => 'inactive'],
    ['last_login[<]' => date('Y-m-d', strtotime('-1 year'))]
);
```

### DELETE删除

从数据库中删除数据。

```php
// 删除单条记录
$affected = $database->delete('users', ['id' => 10]);

// 删除多条记录
$affected = $database->delete('temp_logs', [
    'created_at[<]' => date('Y-m-d', strtotime('-30 days'))
]);

echo "已删除 {$affected} 条记录";
```

## JOIN连接查询

使用JOIN连接多个表进行查询。

```php
// 基本INNER JOIN
$data = $database->select(
    'users',
    ['users.id', 'users.name', 'posts.title'],
    ['users.active' => 1],
    [
        'posts' => [
            'INNER',   // JOIN类型
            'posts',   // 要连接的表
            ['users.id' => 'posts.user_id']  // ON条件
        ]
    ]
);

// LEFT JOIN
$data = $database->select(
    'users',
    ['users.id', 'users.name', 'posts.title'],
    null,
    [
        'posts' => [
            'LEFT',
            'posts',
            ['users.id' => 'posts.user_id']
        ]
    ]
);

// 多表JOIN
$data = $database->select(
    'orders',
    [
        'orders.id',
        'customers.name AS customer_name',
        'products.title AS product_title'
    ],
    ['orders.status' => 'shipped'],
    [
        'customers' => [
            'INNER',
            'customers',
            ['orders.customer_id' => 'customers.id']
        ],
        'order_items' => [
            'INNER',
            'order_items',
            ['orders.id' => 'order_items.order_id']
        ],
        'products' => [
            'LEFT',
            'products',
            ['order_items.product_id' => 'products.id']
        ]
    ]
);
```

### JOIN参数说明

JOIN数组的结构：
```php
[
    '表别名' => [
        '连接类型', // INNER, LEFT, RIGHT, FULL, CROSS
        '表名',
        ['字段1' => '字段2'] // ON条件
    ]
]
```

## 条件查询

Medoo支持多种条件查询方式。

```php
// 基本相等条件
$data = $database->select('users', '*', [
    'status' => 'active'
]);

// AND条件组合
$data = $database->select('users', '*', [
    'status' => 'active',
    'age[>]' => 18
    // 以上条件自动用AND连接
]);

// OR条件
$data = $database->select('users', '*', [
    'OR' => [
        ['status' => 'active'],
        ['vip' => 1]
    ]
]);

// 嵌套AND和OR
$data = $database->select('users', '*', [
    'status' => 'active',
    'OR' => [
        ['age[>]' => 25],
        [
            'AND' => [
                ['age[<]' => 20],
                ['vip' => 1]
            ]
        ]
    ]
]);

// IN条件
$data = $database->select('users', '*', [
    'id' => ['IN', 1, 2, 3, 4, 5]
]);

// NOT IN条件
$data = $database->select('users', '*', [
    'id' => ['NOT IN', 1, 2, 3]
]);

// BETWEEN条件
$data = $database->select('users', '*', [
    'age' => ['BETWEEN', 18, 25]
]);

// NOT BETWEEN条件
$data = $database->select('users', '*', [
    'age' => ['NOT BETWEEN', 30, 40]
]);

// IS NULL条件
$data = $database->select('users', '*', [
    'deleted_at' => null
]);

// IS NOT NULL条件
$data = $database->select('users', '*', [
    'email' => ['NOT NULL']
]);
```

## 事务处理

处理需要保证完整性的多步操作。

```php
try {
    // 开始事务
    $database->beginTransaction();
    
    // 执行多个操作
    $order_id = $database->insert('orders', [
        'customer_id' => 1,
        'amount' => 99.95,
        'created_at' => date('Y-m-d H:i:s')
    ]);
    
    $database->insert('order_items', [
        'order_id' => $order_id,
        'product_id' => 101,
        'quantity' => 1,
        'price' => 99.95
    ]);
    
    // 更新库存
    $database->update('products',
        ['stock[~]' => 'stock - 1'],
        ['id' => 101]
    );
    
    // 提交事务
    $database->commit();
    
    echo "订单处理成功!";
    
} catch (Exception $e) {
    // 回滚事务
    $database->rollBack();
    echo "错误: " . $e->getMessage();
}
```

## 调试与错误处理

获取查询信息和处理错误。

```php
// 执行查询
$data = $database->select('users', '*', ['status' => 'active']);

// 获取最后执行的查询信息
$debug = $database->debug();
echo "SQL: " . $debug['query'] . "\n";
echo "参数: " . print_r($debug['params'], true);

// 错误处理
try {
    $data = $database->select('non_existent_table', '*');
} catch (PDOException $e) {
    echo "数据库错误: " . $e->getMessage();
}

// 使用自定义查询
$statement = $database->query("
    SELECT users.*, COUNT(orders.id) AS order_count
    FROM users
    LEFT JOIN orders ON users.id = orders.user_id
    GROUP BY users.id
    HAVING order_count > 0
");

$results = $statement->fetchAll();
foreach ($results as $row) {
    echo "{$row['name']} has {$row['order_count']} orders\n";
}
```

## 高级用法

一些更高级的使用场景和技巧。

### 获取PDO实例

有时你可能需要直接使用PDO来执行特定操作。

```php
$pdo = $database->pdo();

// 使用原生PDO执行复杂查询
$stmt = $pdo->prepare("SELECT COUNT(*) FROM users WHERE created_at > ?");
$stmt->execute([date('Y-m-d', strtotime('-30 days'))]);
$count = $stmt->fetchColumn();

echo "最近30天注册的用户数: {$count}";
```

### 表别名

在复杂查询中使用表别名。

```php
$data = $database->select(
    ['u' => 'users'],
    ['u.id', 'u.name'],
    ['u.status' => 'active']
);
```

### 组合使用各种功能

```php
// 复杂查询示例
$newUsers = $database->select(
    'users',
    [
        'id',
        'name',
        'email',
        'role_name' => 'roles.name'
    ],
    [
        'users.active' => 1,
        'users.created_at[>]' => date('Y-m-d', strtotime('-30 days')),
        'OR' => [
            ['users.role_id' => 1],
            ['roles.name' => 'admin']
        ]
    ],
    [
        'roles' => [
            'LEFT',
            'roles',
            ['users.role_id' => 'roles.id']
        ]
    ]
);

echo "最近30天注册的活跃用户(普通用户或管理员):\n";
foreach ($newUsers as $user) {
    echo "ID: {$user['id']}, Name: {$user['name']}, Role: {$user['role_name']}\n";
}
```

## 最佳实践

1. **总是使用参数化查询** - Medoo默认使用参数化查询来防止SQL注入。

2. **使用事务保证数据完整性** - 对于涉及多个操作的逻辑，使用事务来保证数据一致性。

3. **处理错误** - 使用try/catch来捕获和处理可能的PDO异常。

4. **关注性能** - 只选择需要的列，合理使用索引。

5. **使用准备好的语句** - 对于重复执行的查询，利用PDO的预处理语句功能。

```php
// 不好的做法: 在循环中执行多次插入
foreach ($users as $user) {
    $database->insert('logs', [
        'user_id' => $user['id'],
        'action' => 'login',
        'created_at' => date('Y-m-d H:i:s')
    ]);
}

// 好的做法: 使用事务和预处理语句
try {
    $database->beginTransaction();
    
    $query = "INSERT INTO logs (user_id, action, created_at) VALUES (?, ?, ?)";
    $stmt = $database->pdo()->prepare($query);
    
    foreach ($users as $user) {
        $stmt->execute([
            $user['id'],
            'login',
            date('Y-m-d H:i:s')
        ]);
    }
    
    $database->commit();
} catch (Exception $e) {
    $database->rollBack();
    echo "Error: " . $e->getMessage();
}
```

6. **定期优化表** - 对于频繁更新的表，定期执行OPTIMIZE TABLE来维护性能。

7. **使用合适的字段类型和索引** - 确保你的数据库表使用了合适的字段类型和必要的索引。

---

这个教程涵盖了精简版Medoo的大部分功能和使用场景。通过这些示例，你应该能够开始使用这个库进行各种数据库操作。记住，Medoo的设计理念是简单、高效，但它也提供了足够的功能来处理大多数常见的数据库操作需求。