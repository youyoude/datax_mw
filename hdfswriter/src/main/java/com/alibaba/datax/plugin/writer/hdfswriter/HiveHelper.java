package com.alibaba.datax.plugin.writer.hdfswriter;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HiveHelper {
    private static final Logger LOG = LoggerFactory.getLogger(HiveHelper.class);

    /**
     * 执行hive -e
     * @param exeCotent
     * @return
     */
    public static int executeHiveCommandE(String exeCotent) {
        int status = 0;
        try {
            if(exeCotent == null){
                status = 2;
                return status;
            }
            List<String> commandList = new ArrayList<String>();
            commandList.add("hive");
            commandList.add("-e");
            commandList.add(exeCotent);
            ProcessBuilder hiveProcessBuilder = new ProcessBuilder(commandList);
            Process hiveProcess = hiveProcessBuilder.start();


            status = hiveProcess.waitFor();//.exitValue();

            InputStream errorStream = hiveProcess.getInputStream();
            String errString = IOUtils.toString(errorStream, StandardCharsets.UTF_8);
            LOG.info(errString);
            LOG.info( "ex status = {}",status);

            errorStream = hiveProcess.getErrorStream();
            errString = IOUtils.toString(errorStream, StandardCharsets.UTF_8);
            LOG.info(errString);
            LOG.info( "ex status = {}",status);

        }catch (Exception e){
            e.printStackTrace();
            status = 1;
        }
        return status;
    }
}
