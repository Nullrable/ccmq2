package cc.mq.broker.consumer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import lombok.Setter;

/**
 * @author nhsoft.lsd
 */
@Setter
@Getter
public class MessageQueue {

    private String topic;

    private Integer queueId;

    private AtomicLong offset = new AtomicLong(-1);

    public MessageQueue(final String topic, final Integer queueId) {
        this.topic = topic;
        this.queueId = queueId;
    }

    public Long ack() {
        return offset.incrementAndGet();
    }
}
