#!/bin/bash
# generating kafka-assembly-0.8.1-deps.jar fails for scala versions 2.10.2 and 2.10.2 so we have to stay with 2.10.1
cd ~/os
rm -rf kafka
export SCALA_VERSION=2.10.1
git clone http://git-wip-us.apache.org/repos/asf/kafka.git kafka
cd kafka
./sbt clean
./sbt "++${SCALA_VERSION} update"
./sbt "++${SCALA_VERSION} assembly-package-dependency"
./sbt "++${SCALA_VERSION} package"
./sbt "++${SCALA_VERSION} test:package"

