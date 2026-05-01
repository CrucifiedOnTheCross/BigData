# Lab 4 ZooKeeper Java

Java implementation for the ZooKeeper laboratory work.

## Implemented tasks

- Barrier synchronization with `/zoo` ephemeral znodes: `zoo.barrier.AnimalRunner`.
- Dining philosophers as independent processes: `zoo.philosophers.PhilosopherMain`.
- Two-phase commit for a replicated register: `zoo.twopc.CoordinatorMain` and `zoo.twopc.RegisterParticipantMain`.
- Integration tests use an embedded ZooKeeper server.

## Test

```powershell
$env:JAVA_HOME=(Resolve-Path '..\..\jdk-17.0.18+8')
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
..\..\apache-maven-3.8.9\bin\mvn.cmd "-Dmaven.repo.local=.m2" test
```

## Run examples

Start ZooKeeper separately on `localhost:2181`, then run several clients.

Barrier:

```powershell
mvn exec:java -Dexec.mainClass=zoo.barrier.AnimalRunner -Dexec.args="lion localhost:2181 3"
mvn exec:java -Dexec.mainClass=zoo.barrier.AnimalRunner -Dexec.args="tiger localhost:2181 3"
mvn exec:java -Dexec.mainClass=zoo.barrier.AnimalRunner -Dexec.args="bear localhost:2181 3"
```

Dining philosophers:

```powershell
mvn exec:java -Dexec.mainClass=zoo.philosophers.PhilosopherMain -Dexec.args="plato localhost:2181 0 5 1000"
```

Two-phase commit:

```powershell
mvn exec:java -Dexec.mainClass=zoo.twopc.CoordinatorMain -Dexec.args="localhost:2181 /ha-register tx-1 3 new-value 10000"
mvn exec:java -Dexec.mainClass=zoo.twopc.RegisterParticipantMain -Dexec.args="node-a localhost:2181 /ha-register tx-1 COMMIT 10000"
```
