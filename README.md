# Cruise

Cruise a machine learning (ML) framework with automatic system configuration, built on top of [Apache REEF]('https://reef.apache.org').
Currently, Cruise consists of a Parameter Server (`Cruise PS`) for asynchronous ML training and a BSP-style engine (`Cruise Pregel`) for graph processing.

## Submodules

* `ps`: An asynchronous ML processing engine based on Parameter Server.
* `pregel`: A BSP-style graph processing engines
* `elastic-tables`: An Elastic Runtime that allows changing system configurations transparently at runtime.

## Requirements

- Java 8 JDK
- Maven 3
- `$ sudo apt-get install libgfortran3` (Ubuntu)

## How to build

```
git clone https://github.com/snuspl/cruise
cd cruise
mvn clean install
```

## Mailing list
All type of discussions such as questions, bug reports and feature requests are always welcome!
Please contact us and share your thoughts by subscribing to cruise-discussion@googlegroups.com
