package cc.mq.broker.meta;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * @author nhsoft.lsd
 */
@Component
@Slf4j
public class CommitedLog {

    private static final String END_FILE_MAGIC_CODE = "ds23dsfs";

    private static final Integer END_FILE_MAGIC_CODE_LENGTH = 8;

    private static final Integer FILE_SIZE = 10 * 1024 * 1024;

    private static final String PATH = "/store/commitlog/";

    private List<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    private String commitLogPath;

    @Resource
    private IndexedStore indexedStore;

    @PostConstruct
    private void init() {
        String home = System.getProperty("mq.home");
        if (home == null) {
            commitLogPath = "C:/Users/nhsof" + PATH;
        } else {
            commitLogPath = home + PATH;
        }

        // 指定目录
        File dir = new File(commitLogPath);
        // 判断是否存在并且是目录
        if (dir.exists() && dir.isDirectory()) {
            // 列出目录下所有文件
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) { // 只处理文件
                        log.info("文件：{}, 加载完成", file.getName());
                        MappedFile mappedFile = MappedFile.createNew(commitLogPath, Long.valueOf(file.getName()), FILE_SIZE);
                        mappedFiles.add(mappedFile);
                    }
                }
            }
        } else {
            log.warn("目录不存在: {}", dir.getAbsolutePath());
        }
    }

    public byte[] get(final String topic, final Integer queueId, final Integer consumeOffset, String consumerGroup) throws IOException {

        IndexedMeta indexedMeta = indexedStore.get(topic, queueId, consumeOffset, consumerGroup);
        if (indexedMeta == null) {
            return null;
        }

        long offset = indexedMeta.getOffset();

        long mode = offset / FILE_SIZE;

        long fileOffset = mode * FILE_SIZE;

        MappedFile mappedFile = MappedFile.findByFileOffset(mappedFiles, fileOffset);

        if (mappedFile == null) {
            log.error("topic {},  queueId {} not found", topic, queueId);
            return null;
        }

        long pos = offset - fileOffset;

        return mappedFile.getMessage((int) pos, indexedMeta.getLength());
    }

    public Boolean write(String topic, Integer queueId, byte[] bytes) throws IOException {

        MappedFile mappedFile = getLastMappedFile(bytes.length);

        Integer lastWritePos = mappedFile.getLastWritePosition().get();

        boolean success = mappedFile.appendMessage(bytes);

        if (!success) {
            return false;
        }

        long offset = mappedFile.getFileFromOffset() + lastWritePos;

        //写入索引
        IndexedMeta indexedLog = new IndexedMeta();
        indexedLog.setLength(bytes.length);
        indexedLog.setOffset(offset);
        indexedLog.setTopic(topic);
        indexedLog.setQueueId(queueId);
        indexedLog.setTag(0L);

        return indexedStore.write(indexedLog);
    }

    public MappedFile getLastMappedFile(Integer msgSize) throws IOException {
        if (mappedFiles.isEmpty()) {

            MappedFile mappedFile = MappedFile.createNew(commitLogPath, 0L, FILE_SIZE);
            mappedFiles.add(mappedFile);

            return mappedFile;
        }

        MappedFile mappedFile = mappedFiles.get(mappedFiles.size() - 1);

        // 预留 8 个字节，用于存储文件结束魔数
        if (mappedFile.isFull(msgSize + END_FILE_MAGIC_CODE_LENGTH)) {
            mappedFile.appendMessageWhenFull(END_FILE_MAGIC_CODE.getBytes(StandardCharsets.UTF_8));
            mappedFile = MappedFile.createNew(commitLogPath, mappedFile.getNextFileOffset(), FILE_SIZE);
            mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    public void close() {
        mappedFiles.forEach(MappedFile::close);
    }
}
