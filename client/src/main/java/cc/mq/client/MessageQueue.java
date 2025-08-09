package cc.mq.client;

import lombok.Data;

/**
 * @author nhsoft.lsd
 */
@Data
public class MessageQueue {

    private String brokerName;

    private String topic;

    private Integer queueId;

}
