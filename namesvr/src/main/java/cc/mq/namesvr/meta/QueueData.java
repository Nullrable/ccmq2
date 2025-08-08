package cc.mq.namesvr.meta;

import lombok.Data;

/**
 * @author nhsoft.lsd
 */
@Data
public class QueueData {

    /**
     * 代理名称.
     */
    private String brokerName;

    /**
     * 队列数量.
     */
    private int queueNums;
}
