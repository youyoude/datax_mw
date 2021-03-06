package com.alibaba.datax.plugin.writer.txtfilewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.unstructuredstorage.writer.UnstructuredStorageWriterUtil;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Created by haiwei.luo on 14-9-17.
 */
public class TxtFileWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration writerSliceConfig = null;
        private List<Configuration>writerSplitedConfig = null;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();
            LOG.info("Job init writerSliceConfig = {}",this.writerSliceConfig.toString());

            this.validateParameter();
            String dateFormatOld = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FORMAT);
            String dateFormatNew = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT);
            if (null == dateFormatNew) {
                this.writerSliceConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.DATE_FORMAT,
                                dateFormatOld);
            }
            if (null != dateFormatOld) {
                LOG.warn("?????????format?????????????????????, ????????????????????????, ???????????????dateFormat?????????, ???????????????????????????dateFormat.");
            }
            UnstructuredStorageWriterUtil
                    .validateParameter(this.writerSliceConfig);
        }

        private void validateParameter() {
            this.writerSliceConfig
                    .getNecessaryValue(
                            com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                            TxtFileWriterErrorCode.REQUIRED_VALUE);

            String path = this.writerSliceConfig.getNecessaryValue(Key.PATH,
                    TxtFileWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: ?????????????????????????????????
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException
                            .asDataXException(
                                    TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                    String.format(
                                            "????????????path: [%s] ???????????????????????????, ????????????????????????, ???????????????????????????.",
                                            path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException
                                .asDataXException(
                                        TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                        String.format("???????????????????????? : [%s] ????????????.",
                                                path));
                    }
                }
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("????????????????????????????????? : [%s] ", path), se);
            }
        }

        @Override
        public void prepare() {
            String path = this.writerSliceConfig.getString(Key.PATH);
            String fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            String writeMode = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.WRITE_MODE);
            // truncate option handler
            if ("truncate".equals(writeMode)) {
                LOG.info(String.format(
                        "??????????????????writeMode truncate, ???????????? [%s] ????????? [%s] ???????????????",
                        path, fileName));
                File dir = new File(path);
                // warn:????????????????????????????????????????????????????????????
                try {
                    if (dir.exists()) {
                        // warn:????????????FileUtils.deleteQuietly(dir);
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        for (File eachFile : filesWithFileNamePrefix) {
                            LOG.info(String.format("delete file [%s].",
                                    eachFile.getName()));
                            FileUtils.forceDelete(eachFile);
                        }
                        // FileUtils.cleanDirectory(dir);
                    }
                } catch (NullPointerException npe) {
                    throw DataXException
                            .asDataXException(
                                    TxtFileWriterErrorCode.Write_FILE_ERROR,
                                    String.format("???????????????????????????????????????????????? : [%s]",
                                            path), npe);
                } catch (IllegalArgumentException iae) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("?????????????????????????????? : [%s]", path));
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("??????????????????????????? : [%s]", path));
                } catch (IOException e) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.Write_FILE_ERROR,
                            String.format("?????????????????? : [%s]", path), e);
                }
            } else if ("append".equals(writeMode)) {
                LOG.info(String
                        .format("??????????????????writeMode append, ???????????????????????????, [%s] ????????????????????????????????????  [%s] ?????????",
                                path, fileName));
            } else if ("nonConflict".equals(writeMode)) {
                LOG.info(String.format(
                        "??????????????????writeMode nonConflict, ???????????? [%s] ???????????????", path));
                // warn: check two times about exists, mkdirs
                File dir = new File(path);
                try {
                    if (dir.exists()) {
                        if (dir.isFile()) {
                            throw DataXException
                                    .asDataXException(
                                            TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "????????????path: [%s] ???????????????????????????, ????????????????????????, ???????????????????????????.",
                                                    path));
                        }
                        // fileName is not null
                        FilenameFilter filter = new PrefixFileFilter(fileName);
                        File[] filesWithFileNamePrefix = dir.listFiles(filter);
                        if (filesWithFileNamePrefix.length > 0) {
                            List<String> allFiles = new ArrayList<String>();
                            for (File eachFile : filesWithFileNamePrefix) {
                                allFiles.add(eachFile.getName());
                            }
                            LOG.error(String.format("?????????????????????: [%s]",
                                    StringUtils.join(allFiles, ",")));
                            throw DataXException
                                    .asDataXException(
                                            TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                            String.format(
                                                    "????????????path: [%s] ???????????????, ????????????????????????????????????.",
                                                    path));
                        }
                    } else {
                        boolean createdOk = dir.mkdirs();
                        if (!createdOk) {
                            throw DataXException
                                    .asDataXException(
                                            TxtFileWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                            String.format(
                                                    "???????????????????????? : [%s] ????????????.",
                                                    path));
                        }
                    }
                } catch (SecurityException se) {
                    throw DataXException.asDataXException(
                            TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                            String.format("??????????????????????????? : [%s]", path));
                }
            } else {
                throw DataXException
                        .asDataXException(
                                TxtFileWriterErrorCode.ILLEGAL_VALUE,
                                String.format(
                                        "????????? truncate, append, nonConflict ????????????, ????????????????????? writeMode ?????? : [%s]",
                                        writeMode));
            }
        }

        @Override
        public void post() {

            String emailAddress = this.writerSliceConfig.getString(Key.EMAIL_ADDRESS);
            String emailTitle = this.writerSliceConfig.getString(Key.EMAIL_TITLE);
            if (emailAddress !=null && emailTitle !=null){
                LOG.info("send email to address {} with title {}",emailAddress,emailTitle);
                String fileName = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
                String type = this.writerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_FORMAT);


                String fileNameBase = fileName.split("\\.")[0];

                List<JSONObject> jsonObjectList = new ArrayList<JSONObject>();

                for (int i=0;i<this.writerSplitedConfig.size();i++){
                    JSONObject jsonObject = new JSONObject();
                    Configuration configuration = this.writerSplitedConfig.get(i);
                    LOG.info(configuration.toString());
                    String oldPath = configuration.getString(Key.PATH) + configuration.getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
                    File file = new File(oldPath);
                    String newPath = String.format("%s%s_%d.%s",configuration.getString(Key.PATH), fileNameBase,i,type);
                    String newName = String.format("%s_%d.%s", fileNameBase,i,type);
                    File newFile = new File(newPath);
                    file.renameTo(newFile);
                    LOG.info(newPath);
                    jsonObject.put("name",newName);
                    jsonObject.put("path",newPath);
                    jsonObjectList.add(jsonObject);
                }

                EmailHelper.pushMsg(emailAddress,emailTitle,jsonObjectList);
            }



        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.info("begin do split...");
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            String filePrefix = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);

            Set<String> allFiles = new HashSet<String>();
            String path = null;
            try {
                path = this.writerSliceConfig.getString(Key.PATH);
                File dir = new File(path);
                allFiles.addAll(Arrays.asList(dir.list()));
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("??????????????????????????? : [%s]", path));
            }

            String fileSuffix;
            for (int i = 0; i < mandatoryNumber; i++) {
                // handle same file name

                Configuration splitedTaskConfig = this.writerSliceConfig
                        .clone();

                String fullFileName = null;
                fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
                while (allFiles.contains(fullFileName)) {
                    fileSuffix = UUID.randomUUID().toString().replace('-', '_');
                    fullFileName = String.format("%s__%s", filePrefix, fileSuffix);
                }
                allFiles.add(fullFileName);

                splitedTaskConfig
                        .set(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME,
                                fullFileName);

                LOG.info(String.format("splited write file name:[%s]",
                        fullFileName));

                writerSplitConfigs.add(splitedTaskConfig);
            }
            LOG.info("end do split.");
            this.writerSplitedConfig =writerSplitConfigs;
            return writerSplitConfigs;
        }

    }

    public static class Task extends Writer.Task {
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private String path;

        private String fileName;

        @Override
        public void init() {
            this.writerSliceConfig = this.getPluginJobConf();

            this.path = this.writerSliceConfig.getString(Key.PATH);
            LOG.info("path = {}",this.path);

            this.fileName = this.writerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.writer.Key.FILE_NAME);
            LOG.info("fileName = {}",this.fileName);

        }

        @Override
        public void prepare() {

        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            LOG.info("begin do write...");
            LOG.info("begin do write... {} ",this.path);
            String fileFullPath = this.buildFilePath();
            LOG.info(String.format("write to file : [%s]", fileFullPath));

            OutputStream outputStream = null;
            try {
                File newFile = new File(fileFullPath);
                newFile.createNewFile();
                outputStream = new FileOutputStream(newFile);
                UnstructuredStorageWriterUtil.writeToStream(lineReceiver,
                        outputStream, this.writerSliceConfig, this.fileName,
                        this.getTaskPluginCollector());
            } catch (SecurityException se) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("???????????????????????????  : [%s]", this.fileName));
            } catch (IOException ioe) {
                throw DataXException.asDataXException(
                        TxtFileWriterErrorCode.Write_FILE_IO_ERROR,
                        String.format("???????????????????????? : [%s]", this.fileName), ioe);
            } finally {
                IOUtils.closeQuietly(outputStream);
            }
            LOG.info("end do write");
        }

        private String buildFilePath() {
            boolean isEndWithSeparator = false;
            switch (IOUtils.DIR_SEPARATOR) {
            case IOUtils.DIR_SEPARATOR_UNIX:
                isEndWithSeparator = this.path.endsWith(String
                        .valueOf(IOUtils.DIR_SEPARATOR));
                break;
            case IOUtils.DIR_SEPARATOR_WINDOWS:
                isEndWithSeparator = this.path.endsWith(String
                        .valueOf(IOUtils.DIR_SEPARATOR_WINDOWS));
                break;
            default:
                break;
            }
            if (!isEndWithSeparator) {
                this.path = this.path + IOUtils.DIR_SEPARATOR;
            }
            return String.format("%s%s", this.path, this.fileName);
        }

        @Override
        public void post() {

        }

        @Override
        public void destroy() {

        }
    }
}
