package cn.huohua.datax.plugin.writer.rocketmqwriter;

public class Constant {


    //最大允许发送字节数
    public static final int maxMessageSize = 1024 * 1024 * 10; // 4M
    //同步发送消息，发送失败时再尝试发送2次数
    public static final int retryTimesWhenSendFailed = 2;
    // 当发送的消息大于 4K 时，开始压缩消息。
    public static final int compressMsgBodyOverHowmuch = 1024 * 4;
    // 发送消息超时时间
    public static final int sendMsgTimeout = 3000;
    // 创建 Topic 默认的4个队列
    public static final int defaultTopicQueueNums = 10;

    //producer 组名
    public static final String producerGroup = "default_group";

}
