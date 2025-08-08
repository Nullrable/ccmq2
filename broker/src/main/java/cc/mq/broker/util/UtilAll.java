package cc.mq.broker.util;

/**
 * @author nhsoft.lsd
 */
public class UtilAll {

    public static final String offset2FileName(final Long offset) {
        return String.format("%020d", offset);
    }
}
