package cc.mq.broker.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.xml.parsers.SAXParser;
import lombok.Data;
import org.springframework.stereotype.Component;

/**
 * @author nhsoft.lsd
 */
@Component
public class SubscriptionStore {

    Map<String /* topic */, Subscription> subscriptions = new ConcurrentHashMap<>();

    public void sub(String topic, String consumerGroup, String consumerId) {

        Subscription subscription = subscriptions.get(topic);
        if (subscription == null) {
            subscription = new Subscription();
            subscription.setTopic(topic);
            subscription.addConsumer(consumerGroup, consumerId);
            subscriptions.put(topic, subscription);
        } else {
            subscription.addConsumer(consumerGroup, consumerId);
        }
    }

    public void unsub(final String topic, final String consumerGroup, final String consumerId) {

        Subscription subscription = subscriptions.get(topic);
        if (subscription != null) {
            subscription.removeConsumer(consumerGroup, consumerId);
        }
    }

    public Subscription get(final String topic) {
        return subscriptions.get(topic);
    }
}
