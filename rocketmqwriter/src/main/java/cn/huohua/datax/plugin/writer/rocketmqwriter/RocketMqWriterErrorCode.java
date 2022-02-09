package cn.huohua.datax.plugin.writer.rocketmqwriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum  RocketMqWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("RocketMqWriter-00", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("KafkaWriteErrorCode-01", "您填写的参数值不合法."),
    CONF_ERROR("KafkaWriteErrorCode-00", "您的配置错误."),
    NO_INDEX_VALUE("RocketMqWriter-06","column 中 缺失了必须填写的参数 Index" ),
    NO_KEY_NAME_VALUE("RocketMqWriter-06","column 中 缺失了必须填写的参数  keyName" ),

    ;

    private final String code;

    private final String description;

    private RocketMqWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
