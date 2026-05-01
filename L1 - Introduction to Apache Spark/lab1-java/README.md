# Lab 1 Java Solution

Java-реализация первой лабораторной по Apache Spark без Spring Boot. Для этой работы Spring Boot не даёт заметной пользы, потому что приложение запускается как одноразовый batch job, а не как веб-сервис.

## Важное замечание по Java

Проект протестирован на **JDK 17**. На установленной у тебя **JDK 25** локальный Spark/Hadoop стек падает из-за несовместимости `Subject.getSubject()` и модульных ограничений JDK.

В workspace уже подготовлена совместимая JDK:

```text
..\..\jdk-17.0.18+8
```

## Что реализовано

Приложение решает все 5 задачи из [L1_Apache_Spark_Tasks.md](../L1_Apache_Spark_Tasks.md):

1. находит велосипед с максимальным суммарным временем поездок;
2. находит максимальное геодезическое расстояние между станциями;
3. восстанавливает путь велосипеда с максимальным временем пробега;
4. считает количество велосипедов в системе;
5. находит пользователей, потративших на поездки более 3 часов.

Примечание по пункту 5: в исходных данных нет отдельного `user_id`, поэтому пользователь интерпретируется как пара `subscription_type + zip_code`.

## Структура

- `src/main/java` — код приложения;
- `src/test/java` — unit и Spark-local тесты;
- `pom.xml` — Maven-конфигурация.

## Запуск тестов

Из папки проекта:

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

После сборки зависимости будут скопированы в `target/dependency`.

## Запуск приложения

Проверенный локальный запуск с данными репозитория по умолчанию:

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
  '-cp','target\lab1-java-1.0-SNAPSHOT.jar;target\dependency\*',
  'ru.bigdata.lab1.Lab1Application'
)
& "$env:JAVA_HOME\bin\java.exe" @javaArgs
```

Если нужно передать свои файлы, добавь аргументы после имени класса:

```powershell
& "$env:JAVA_HOME\bin\java.exe" @javaArgs 'yarn' 'trips.csv' 'stations.csv'
```
