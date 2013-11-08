#!/bin/bash
export KAFKA_HOME=/var/opt/kafka
rm -rf /tmp/zookeeper
rm -rf /tmp/kafka-logs*
for i in {0..2}
do
	let real_port=$i+9092 
	sed "{
		/^broker.id=/ s/0/${i}/
		/^port=/ s/9092/${real_port}/
		/^log.dirs=/ s/\/tmp\/kafka-logs/\/tmp\/kafka-logs-${i}/
        
             }" ${KAFKA_HOME}/config/server.properties > ${KAFKA_HOME}/config/server-${i}.properties
done

gnome-terminal -e "env SCALA_VERSION=2.10 $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties"

for i in {0..2}
do
  let JMX_PORT=$i+9999
  gnome-terminal -e "env JMX_PORT=${JMX_PORT} SCALA_VERSION=2.10 $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server-${i}.properties" --tab-with-profile=stayOpen
done
[[ $($KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --list) =~ "tracking.cached.requests" ]] || $($KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic tracking.cached.requests --partitions 3 --replication-factor 2)