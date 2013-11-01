export KAFKA_HOME=/var/opt/kafka
gnome-terminal -e "$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties"
gnome-terminal -e "env JMX_PORT=9999 $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server0.properties"
gnome-terminal -e "env JMX_PORT=10000 $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server1.properties"
gnome-terminal -e "env JMX_PORT=10001 $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server2.properties"

[[ $($KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --list) =~ "tracking.cached.requests" ]] || $($KAFKA_HOME/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic tracking.cached.requests --partitions 3 --replication-factor 2)