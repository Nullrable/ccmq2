package cc.mq.namesvr.meta;

import cc.mq.namesvr.util.MixAll;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;

/**
 * @author nhsoft.lsd
 */
@Setter
@Getter
public class BrokerData {

    /**
     * 集群.
     */
    private String cluster;

    /**
     * 代理名称.
     */
    private String brokerName;

    /**
     * 服务地址.
     */
    private HashMap<Long, String> brokerAddrTable;

    /**
     * 区域.
     */
    private String zoneName;

    private Random random = new Random();

    public String selectOneBrokerAddr() {
        String masterAddr = brokerAddrTable.get(MixAll.MASTER_ID);
        if (masterAddr != null && !masterAddr.isBlank()) {
            return masterAddr;
        }
        List<String> brokerAddrList = brokerAddrTable.values().stream().toList();
        return brokerAddrList.get(random.nextInt(brokerAddrList.size()));
    }
}
