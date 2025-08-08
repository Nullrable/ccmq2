package cc.mq.broker.meta;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.stereotype.Component;

/**
 * @author nhsoft.lsd
 */
@Component
public class IndexedStore {

    protected final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, IndexedQueue>> indexedQueueTable = new ConcurrentHashMap<>();

    public Boolean write(IndexedMeta log) throws IOException {

        IndexedQueue indexedQueue = null;
        ConcurrentMap<Integer/* queueId */, IndexedQueue> mappedQueue = indexedQueueTable.get(log.getTopic());

        if (mappedQueue == null) {
            mappedQueue = new ConcurrentHashMap<>();
            indexedQueue = new IndexedQueue(log.getTopic(), log.getQueueId());
            mappedQueue.put(log.getQueueId(), indexedQueue);
            indexedQueueTable.put(log.getTopic(), mappedQueue);
        } else {
            indexedQueue =  mappedQueue.get(log.getQueueId());
            if (indexedQueue == null) {
                indexedQueue = new IndexedQueue(log.getTopic(), log.getQueueId());
                mappedQueue.put(log.getQueueId(), indexedQueue);
            }
        }

        return indexedQueue.write(log);
    }

    public IndexedMeta get(final String topic, final Integer queueId, final Integer consumeOffset) {

        IndexedQueue indexedQueue = null;
        ConcurrentMap<Integer/* queueId */, IndexedQueue> mappedQueue = indexedQueueTable.get(topic);

        if (mappedQueue == null) {
          return null;
        } else {
            indexedQueue =  mappedQueue.get(queueId);
            if (indexedQueue == null) {
               return null;
            }
        }
        return indexedQueue.get(consumeOffset);
    }
}
