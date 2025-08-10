package cc.mq.broker.controller;

import cc.mq.broker.meta.CCMessage;
import cc.mq.broker.meta.CommitedLogStore;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author nhsoft.lsd
 */
@RestController
@RequestMapping("/producer")
public class ProducerController {

    @Resource
    private CommitedLogStore commitedLogStore;

    @PostMapping("send")
    void send(@RequestParam String topic, @RequestParam("queue_id") Integer queueId, @RequestBody CCMessage msg) {
        commitedLogStore.write(topic, queueId, msg);
    }
}
