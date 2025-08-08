package cc.mq.namesvr.meta;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author nhsoft.lsd
 */
public class RouteInfo {

    private final Map<String/* topicName */, Map<String/* brokerName */, QueueData>> topicQueueTable; // topic - queue

    private final Map<String/* brokerName */, BrokerData> brokerAddrTable;

    public RouteInfo(final Map<String, String> queueTable, final Map<String, BrokerData> brokerAddrTable) {
        this.topicQueueTable = new ConcurrentHashMap<>(1024);
        this.brokerAddrTable = new ConcurrentHashMap<>(128);
    }
}
