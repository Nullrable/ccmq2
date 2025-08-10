package cc.mq.broker.meta;

import cc.mq.broker.consumer.Subscription;
import cc.mq.broker.consumer.SubscriptionStore;
import jakarta.annotation.Resource;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author nhsoft.lsd
 */
@Slf4j
@Component
public class IndexedStore {

    protected final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, IndexedQueue>> indexedQueueTable = new ConcurrentHashMap<>();

    @Resource
    private SubscriptionStore subscriptionStore;

    public Long getTareIndex(String topic, Integer queueId) {
        ConcurrentMap<Integer/* queueId */, IndexedQueue> mappedQueue = indexedQueueTable.get(topic);
        if (mappedQueue == null) {
            return 0L;
        }
        IndexedQueue indexedQueue = mappedQueue.get(queueId);
        if (indexedQueue == null) {
            return 0L;
        }
        return indexedQueue.getTareIndex();
    }

    public Boolean write(IndexedMeta log) throws IOException {

        IndexedQueue indexedQueue = null;
        ConcurrentMap<Integer/* queueId */, IndexedQueue> mappedQueue = indexedQueueTable.get(log.getTopic());

        if (mappedQueue == null) {
            mappedQueue = new ConcurrentHashMap<>();
            indexedQueue = new IndexedQueue(log.getTopic(), log.getQueueId());
            mappedQueue.put(log.getQueueId(), indexedQueue);
            indexedQueueTable.put(log.getTopic(), mappedQueue);
        } else {
            indexedQueue = mappedQueue.get(log.getQueueId());
            if (indexedQueue == null) {
                indexedQueue = new IndexedQueue(log.getTopic(), log.getQueueId());
                mappedQueue.put(log.getQueueId(), indexedQueue);
            }
        }

        return indexedQueue.write(log);
    }

    public IndexedMeta get(final String topic, final Integer queueId, final Integer consumeOffset, final String consumerGroup) {

        IndexedQueue indexedQueue = null;
        ConcurrentMap<Integer/* queueId */, IndexedQueue> mappedQueue = indexedQueueTable.get(topic);

        if (mappedQueue == null) {
            mappedQueue = new ConcurrentHashMap<>();
            Subscription subscription = subscriptionStore.get(topic);
            if (subscription == null) {
                log.warn("topic {} not found", topic);
                return null;
            }

            if (subscription.exists(consumerGroup)) {
                indexedQueue = new IndexedQueue(topic, queueId);
                mappedQueue.put(queueId, indexedQueue);
                indexedQueueTable.put(topic, mappedQueue);
            }
        } else {
            indexedQueue = mappedQueue.get(queueId);
            if (indexedQueue == null) {
                Subscription subscription = subscriptionStore.get(topic);
                if (subscription.exists(consumerGroup)) {
                    indexedQueue = new IndexedQueue(topic, queueId);
                    mappedQueue.put(queueId, indexedQueue);
                }
            }
        }
        return indexedQueue.get(consumeOffset);
    }
}
