#!/bin/bash
pushd /home/rsi/os/kafka
./sbt clean
./sbt update
./sbt "++2.10.1 package"
./sbt "++2.10.1 assembly-package-dependency"
popd
