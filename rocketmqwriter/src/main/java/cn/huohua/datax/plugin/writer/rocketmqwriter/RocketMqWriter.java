package cn.huohua.datax.plugin.writer.rocketmqwriter;


import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import cn.huohua.datax.plugin.writer.rocketmqwriter.Constant;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.fastjson.JSONObject;


import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;


public class RocketMqWriter extends Writer {

    public static class Job extends Writer.Job {
        private static final Logger log = LoggerFactory.getLogger(Job.class);

        private Configuration configuration = null;

        @Override
        public void init() {
            this.configuration =super.getPluginJobConf();
            log.info("rocketmqwriter writer params:{}", this.configuration.toJSON());

            this.configuration.getNecessaryValue(Key.MQ_NAMESERVER, RocketMqWriterErrorCode.REQUIRED_VALUE);
            this.configuration.getNecessaryValue(Key.TOPIC, RocketMqWriterErrorCode.REQUIRED_VALUE);
            this.configuration.getNecessaryValue(Key.MQ_COLUMNS, RocketMqWriterErrorCode.REQUIRED_VALUE);
            validateColumns();

            //还需检查topic 是否存在

        }


        @Override
        public List<Configuration> split( int mandatoryNumber){
            List<Configuration> splitResultConfigs = new ArrayList<Configuration>();
            for (int i =0;i<mandatoryNumber;i++){
                Configuration splitConfig = this.configuration.clone();
                splitResultConfigs.add(splitConfig);
            }

            return splitResultConfigs;
        }

        @Override
        public void destroy() {

        }

        private void validateColumns(){
            List<Configuration> columns = this.configuration.getListConfiguration(Key.MQ_COLUMNS);
            for (Configuration eachColumnConf : columns) {
                Integer columnIndex = eachColumnConf.getInt(Key.INDEX);
                String columnName = eachColumnConf.getString(Key.KEY_NAME);
                if (null == columnIndex ) {
                    throw DataXException.asDataXException(
                            RocketMqWriterErrorCode.NO_INDEX_VALUE,
                            RocketMqWriterErrorCode.NO_INDEX_VALUE.getDescription());
                }
                if (null == columnName){
                    throw DataXException.asDataXException(
                            RocketMqWriterErrorCode.NO_KEY_NAME_VALUE,
                            RocketMqWriterErrorCode.NO_KEY_NAME_VALUE.getDescription());
                }

            }

        }


    }

    public static class Task extends Writer.Task{

        private static final Logger log = LoggerFactory.getLogger(Job.class);
        private Configuration configuration = null;

        private String topic = null;
        private String nameServerAddress = null;
        private List<Configuration> coulmns = null;

        protected int batchSize;
        protected int batchByteSize;

        private DefaultMQProducer mqProducer;

        @Override
        public void init() {
            this.configuration =super.getPluginJobConf();

            this.topic = this.configuration.getString(Key.TOPIC);
            this.nameServerAddress = this.configuration.getString(Key.MQ_NAMESERVER);
            this.coulmns = this.configuration.getListConfiguration(Key.MQ_COLUMNS);

            if (this.configuration.getInt(Key.BATCH_SIZE) == null){
                this.batchSize =100;
            }else {
                this.batchSize = this.configuration.getInt(Key.BATCH_SIZE);
            }


            try {
//                INFO
                System.setProperty("rocketmq.client.logLevel", "WARN");
//        System.setProperty("rocketmq.client.logRoot", "/opt/bdp/data01/rocketmq/logs/rocketmqlogs" + "/logs/rocketmqlogs");
                System.setProperty("rocketmq.client.logUseSlf4j", "true"); //禁用rocketmq自加载日志

                this.mqProducer =new DefaultMQProducer(Constant.producerGroup);
                //如果需要同一个jvm中不同的producer往不同的mq集群发送消息，需要设置不同的instanceName
                this.mqProducer.setInstanceName("data_push_spark" + this.getTaskId());


                this.mqProducer.setNamesrvAddr(this.nameServerAddress);
                this.mqProducer.setCompressMsgBodyOverHowmuch(Constant.compressMsgBodyOverHowmuch);
                this.mqProducer.setSendMsgTimeout(Constant.sendMsgTimeout);
                this.mqProducer.setRetryTimesWhenSendFailed(Constant.retryTimesWhenSendFailed);
                this.mqProducer.setMaxMessageSize(Constant.maxMessageSize);
                this.mqProducer.setDefaultTopicQueueNums(Constant.defaultTopicQueueNums);
                //Launch the instance.
                this.mqProducer.start();

                log.info(String.format("producer is start ! groupName:[%s],namesrvAddr:[%s]"
                        , mqProducer.getProducerGroup(), mqProducer.getNamesrvAddr()));

//                Set<String> topicSet = this.mqProducer.getDefaultMQProducerImpl().getTopicPublishInfoTable();
//                System.out.println(topicSet.toString());
//                log.info(String.format("topic 是否存在 %s", topicSet.contains("dada")) );


            } catch (Exception ex) {
                log.error(String.format("producer is error {}"
                        , ex.getMessage(), ex));
                throw DataXException.asDataXException(RocketMqWriterErrorCode.ILLEGAL_VALUE,
                        String.format("RocketMq 连接错误"));
            }


        }


        @Override
        public void startWrite(RecordReceiver recordReceiver)  {

            Record record = null;
            List<Record> writeBuffer = new ArrayList<Record>(this.batchSize);
            log.info("startWrite getTaskId=  {}", this.getTaskId());
            log.info("startWrite getTaskGroupId=  {}", this.getTaskGroupId());

            int bufferBytes = 0;
            while ((record = recordReceiver.getFromReader()) != null) {

                writeBuffer.add(record);
                bufferBytes += record.getMemorySize();

                if(writeBuffer.size() >= batchSize ||bufferBytes >= batchByteSize){
                    try {
                        sendBatchRecord(writeBuffer);

                    }catch (Exception e){
                        e.printStackTrace();
                    }
                    writeBuffer.clear();
                    bufferBytes = 0;
                }
            }
            if(!writeBuffer.isEmpty()){
                try {
                    sendBatchRecord(writeBuffer);

                }catch (Exception e){
                    e.printStackTrace();
                }
                writeBuffer.clear();
                bufferBytes = 0;
            }
            this.mqProducer.shutdown();

        }

        protected void sendBatchRecord(List<Record> writeBuffer)
                throws MQClientException, InterruptedException, UnsupportedEncodingException, RemotingException, MQBrokerException {

            for(Record record: writeBuffer){
                JSONObject jsonObject =new JSONObject();

                for (Configuration eachColumnConf : this.coulmns) {
                    Integer columnIndex = eachColumnConf.getInt(Key.INDEX);
                    String columnName = eachColumnConf.getString(Key.KEY_NAME);
                    jsonObject.put(columnName,record.getColumn(columnIndex).getRawData());

                }


                Message msg = new Message(topic, null, getTraceId(topic), (jsonObject.toString()).getBytes(RemotingHelper.DEFAULT_CHARSET));
                try {
                    SendResult sendResult = this.mqProducer.send(msg);
                    SendStatus sendStatus = sendResult.getSendStatus();

                    log.debug("topic : " + topic + " 发送的消息ID: " + sendResult.getMsgId() + "--- 发送消息的状态：" + sendStatus + " 发送消息 : " + jsonObject.toString() );

                } catch (DataXException e){
                    throw DataXException.asDataXException(RocketMqWriterErrorCode.ILLEGAL_VALUE,"");
                }

            }


        }



        /**
         * 生成消息唯一id
         *
         * @param topic
         * @return
         */
        private static String getTraceId(String topic) {
            return topic + System.currentTimeMillis() + Math.abs(ThreadLocalRandom.current().nextInt());
//            return topic + System.currentTimeMillis() ;
        }


            @Override
        public void destroy() {
            if(this.mqProducer != null){
//                this.mqProducer.shutdown();
            }

        }

    }

}
