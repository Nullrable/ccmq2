package cc.mq.broker.meta;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
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

    private static final String END_FILE_MAGIC_CODE = "-875286124";

    public final static int MESSAGE_MAGIC_CODE = -626843481;

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
            File[] filearray = dir.listFiles();
            if (filearray != null) {
               List<File> files = Arrays.asList(filearray);
                //根据文件名进行排序
                files.sort(Comparator.comparing(File::getName));
                for (File file : files) {
                    if (file.isFile()) { // 只处理文件
                        log.info("文件：{}, 加载完成", file.getName());
                        MappedFile mappedFile = MappedFile.doLoad(commitLogPath, Long.valueOf(file.getName()), FILE_SIZE);
                        mappedFiles.add(mappedFile);

                        int pos = 0;
                        while (true) {
                            int size = mappedFile.getBuffer().getInt(pos);
                            if (size <= 0) break; // 空洞
                            int magicCode = mappedFile.getBuffer().getInt(pos + 4);
                            if (magicCode != MESSAGE_MAGIC_CODE) break; // 无效数据

                            pos += size; // 跳到下一条消息
                        }
                        mappedFile.setLastWritePosition(pos);
                        mappedFile.position(pos);
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

        byte[] bytes = mappedFile.getMessage((int) pos, indexedMeta.getLength());

        if (bytes.length <= 8) {
            log.error("message not valid, topic {}, queueId {}, offset {}", topic, queueId, consumeOffset);
            return null;
        }
        byte[] result = new byte[bytes.length - 8];
        System.arraycopy(bytes, 8, result, 0, result.length);
        return result;

    }

    public Boolean write(String topic, Integer queueId, byte[] bytes) throws IOException {

        MappedFile mappedFile = getLastMappedFile(bytes.length);

        Integer lastWritePos = mappedFile.getLastWritePosition().get();

        byte[] result = mergeCommitedLog(bytes);

        boolean success = mappedFile.appendMessage(result);

        if (!success) {
            return false;
        }

        long offset = mappedFile.getFileFromOffset() + lastWritePos;

        //写入索引
        IndexedMeta indexedLog = new IndexedMeta();
        indexedLog.setLength(result.length);
        indexedLog.setOffset(offset);
        indexedLog.setTopic(topic);
        indexedLog.setQueueId(queueId);
        indexedLog.setTag(0L);

        return indexedStore.write(indexedLog);
    }

    private static byte[] mergeCommitedLog(final byte[] bodys) {
        byte[] magicCodes = ByteBuffer.allocate(4).putInt(MESSAGE_MAGIC_CODE).array();

        int totalSize = magicCodes.length + bodys.length + 4;

        byte[] totalSizes = ByteBuffer.allocate(4).putInt(totalSize).array();

        byte[] result = new byte[totalSizes.length + magicCodes.length + bodys.length];

        // 2. 按顺序复制
        int pos = 0;
        System.arraycopy(totalSizes, 0, result, pos, totalSizes.length);
        pos += totalSizes.length;
        System.arraycopy(magicCodes, 0, result, pos, magicCodes.length);
        pos += magicCodes.length;
        System.arraycopy(bodys, 0, result, pos, bodys.length);

        return result;
    }

    public MappedFile getLastMappedFile(Integer msgSize) throws IOException {
        if (mappedFiles.isEmpty()) {

            MappedFile mappedFile = MappedFile.doLoad(commitLogPath, 0L, FILE_SIZE);
            mappedFiles.add(mappedFile);

            return mappedFile;
        }

        MappedFile mappedFile = mappedFiles.get(mappedFiles.size() - 1);

        // 预留 8 个字节，用于存储文件结束魔数
        if (mappedFile.isFull(msgSize + END_FILE_MAGIC_CODE_LENGTH)) {
            mappedFile.appendMessageWhenFull(END_FILE_MAGIC_CODE.getBytes(StandardCharsets.UTF_8));
            mappedFile = MappedFile.doLoad(commitLogPath, mappedFile.getNextFileOffset(), FILE_SIZE);
            mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    public void close() {
        mappedFiles.forEach(MappedFile::close);
    }
}
