package com.test.simple;

import com.google.common.collect.ImmutableList;
import kafka.cluster.Broker;

public class BrokerInfo {
    private final String host;
    private final int port;

    public BrokerInfo(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public BrokerInfo(Broker replica) {
        this.host = replica.host();
        this.port = replica.port();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public static ImmutableList<BrokerInfo> brokerInfos(String brokersString) {
        ImmutableList.Builder<BrokerInfo> builder = ImmutableList.<BrokerInfo>builder();
        String[] brokerStrings = brokersString.split(",");
        for (String brokerString : brokerStrings) {
            String[] hostAndPort = brokerString.split(":");
            BrokerInfo brokerInfo = new BrokerInfo(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
            builder.add(brokerInfo);
        }
        return builder.build();
    }
}
