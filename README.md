# Java ORM code for study

#Usage:

1. Add annotation to the table class: `Table`, `Id` and `Column`;
2. Add a child class for the `JdbcUtils` class;
3. Initialization the child class by uri, username, password and parameters array;
4. Use `JdbcUtils#Connection()` function to connect to the MySQL database;
5. You can use `insert`, `updateById`, `deleteById` and `selectById`.

#Note:
1. If you MySQL database have used `datetime`, it can only be converted into `String`;
2. It supports `int`, `Integer`, `double`, 'Double` and `String`.

#Thanks:
This code is mainly referenced [【Java进阶】实现自己的ORM框架](https://blog.csdn.net/liyazhou0215/article/details/77431561).
