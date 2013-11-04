#!/bin/bash
git clone http://git-wip-us.apache.org/repos/asf/kafka.git kafka
cd kafka
./sbt update
./sbt package
./sbt assembly-package-dependency
./sbt +package

