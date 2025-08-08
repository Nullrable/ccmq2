package cc.mq.broker.meta;

import lombok.Data;

/**
 * @author nhsoft.lsd
 */
@Data
public class IndexedMeta {

    private String topic;

    private Integer queueId;

    private Long offset;

    private Integer length;

    private Long tag;
}
