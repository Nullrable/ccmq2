package cc.mq.broker.meta;

import com.alibaba.fastjson.JSON;
import jakarta.annotation.Resource;
import java.io.IOException;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author nhsoft.lsd
 */
@Slf4j
@Component
public class CommitedLogStore {

    @Resource
    private CommitedLog commitedLog;

    public void write(String topic, Integer queueId, CCMessage msg) {
        try {
            byte[] content = JSON.toJSONBytes(msg);
            log.info("write bytes " + Arrays.toString(content));
            commitedLog.write(topic, queueId, content);
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
            log.info("read bytes " + Arrays.toString(bytes));
            log.info("read string " + new String(bytes));
            return JSON.parseObject(bytes, CCMessage.class);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
