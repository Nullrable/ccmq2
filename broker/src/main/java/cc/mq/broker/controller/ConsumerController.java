package cc.mq.broker.controller;

import cc.mq.broker.consumer.MessageQueueStore;
import cc.mq.broker.consumer.SubscriptionStore;
import cc.mq.broker.meta.CCMessage;
import cc.mq.broker.meta.CommitedLogStore;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author nhsoft.lsd
 */
@RestController("/consumer")
public class ConsumerController {

    @Resource
    private SubscriptionStore subscriptionStore;

    @Resource
    private CommitedLogStore commitedLogStore;

    @Resource
    private MessageQueueStore messageQueueStore;

    @PostMapping("/sub")
    public void sub(@RequestParam("topic") String topic, @RequestParam("consumer_group") String consumerGroup, @RequestParam("consumer_id") String consumerId) {
        subscriptionStore.sub(topic, consumerGroup, consumerId);
    }

    @PostMapping("/unsub")
    public void unsub(@RequestParam("topic") String topic, @RequestParam("consumer_group") String consumerGroup, @RequestParam("consumer_id") String consumerId) {
        subscriptionStore.unsub(topic, consumerGroup, consumerId);
    }

    @PostMapping("/recv")
    @ResponseBody
    public CCMessage recv(@RequestParam("topic") String topic, @RequestParam("queue_id") Integer queueId, @RequestParam("offset") Integer offset) {
        return commitedLogStore.get(topic, queueId, offset);
    }

    @PostMapping("/ack")
    public Long ack(@RequestParam("topic") String topic, @RequestParam("queue_id") Integer queueId) {
        return messageQueueStore.ack(topic, queueId);
    }
}
