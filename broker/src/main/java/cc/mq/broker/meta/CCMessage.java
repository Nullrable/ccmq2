package cc.mq.broker.meta;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;

/**
 * @author nhsoft.lsd
 */
@Data
public class CCMessage {

    private String body;

    private String msgId;

    private Map<String, String> headers = new HashMap<>();

    private Map<String, String> properties = new HashMap<>();

}
