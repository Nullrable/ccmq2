package cc.mq.broker.meta;

import com.alibaba.fastjson.JSON;
import jakarta.annotation.Resource;
import java.io.IOException;
import org.springframework.stereotype.Component;

/**
 * @author nhsoft.lsd
 */
@Component
public class CommitedLogStore {

    @Resource
    private CommitedLog commitedLog;

    public void write(String topic, Integer queueId, CCMessage msg) {
        try {
            commitedLog.write(topic, queueId, JSON.toJSONBytes(msg));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public CCMessage get(String topic, Integer queueId, Integer offset, String consumerGroup) {
        try {
            byte[] bytes = commitedLog.get(topic, queueId, offset, consumerGroup);
            if (bytes == null) {
                return null;
            }
            return JSON.parseObject(bytes, CCMessage.class);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
