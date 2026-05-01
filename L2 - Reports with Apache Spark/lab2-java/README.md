# Lab 2 Java Solution

Java-реализация второй лабораторной на Apache Spark SQL.

## Что делает проект

Приложение:

1. считывает Stack Overflow XML файл постов;
2. извлекает вопросы (`PostTypeId = 1`);
3. сопоставляет теги вопросов со списком языков из `programming-languages.csv`;
4. считает число вопросов по каждому языку за каждый год с 2010 по 2020;
5. строит `top-10` языков по итогам каждого года;
6. сохраняет отчёт в формате Apache Parquet.

Принятая метрика популярности: **количество вопросов Stack Overflow с тегом языка за год**.

## Важное замечание по Java

Проект протестирован на **JDK 17**. Для локального Spark не используется установленная системная JDK 25.

Подготовленная JDK:

```text
..\..\jdk-17.0.18+8
```

## Запуск тестов

```powershell
$env:JAVA_HOME=(Resolve-Path '..\..\jdk-17.0.18+8')
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
..\..\apache-maven-3.8.9\bin\mvn.cmd "-Dmaven.repo.local=.m2" test
```

## Сборка

```powershell
$env:JAVA_HOME=(Resolve-Path '..\..\jdk-17.0.18+8')
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
..\..\apache-maven-3.8.9\bin\mvn.cmd "-Dmaven.repo.local=.m2" clean package
```

## Локальный запуск

По умолчанию используется:

- `..\..\data\posts_sample.xml`
- `..\..\data\programming-languages.csv`
- выходной каталог `target\language-report-parquet`

Проверенный запуск:

```powershell
$env:JAVA_HOME=(Resolve-Path '..\..\jdk-17.0.18+8')
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
$javaArgs = @(
  '--add-opens','java.base/java.lang=ALL-UNNAMED',
  '--add-opens','java.base/java.lang.invoke=ALL-UNNAMED',
  '--add-opens','java.base/java.lang.reflect=ALL-UNNAMED',
  '--add-opens','java.base/java.io=ALL-UNNAMED',
  '--add-opens','java.base/java.net=ALL-UNNAMED',
  '--add-opens','java.base/java.nio=ALL-UNNAMED',
  '--add-opens','java.base/java.util=ALL-UNNAMED',
  '--add-opens','java.base/java.util.concurrent=ALL-UNNAMED',
  '--add-opens','java.base/sun.nio.ch=ALL-UNNAMED',
  '--add-exports','java.base/sun.security.action=ALL-UNNAMED',
  '-Dio.netty.tryReflectionSetAccessible=true',
  '-cp','target\lab2-java-1.0-SNAPSHOT.jar;target\dependency\*',
  'ru.bigdata.lab2.Lab2Application'
)
& "$env:JAVA_HOME\bin\java.exe" @javaArgs
```

Запись Parquet на Windows уже настроена внутри приложения: проект использует локальную Hadoop FS-реализацию без вызовов `winutils.exe`, поэтому отдельный `HADOOP_HOME` для этой лабораторной не нужен.

## Запуск со своими путями

```powershell
& "$env:JAVA_HOME\bin\java.exe" @javaArgs 'yarn' 'posts.xml' 'programming-languages.csv' 'report-parquet'
```
