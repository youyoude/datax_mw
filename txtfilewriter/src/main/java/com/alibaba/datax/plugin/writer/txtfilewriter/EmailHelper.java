package com.alibaba.datax.plugin.writer.txtfilewriter;

import com.alibaba.fastjson.JSONObject;
import org.apache.avro.data.Json;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;


public class EmailHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TxtFileWriter.Job.class);

    public static byte[] getFileContent(String fileName){
//        File file =new File("/Users/mayunqi/testzhuser11_0.csv");
        File file =new File(fileName);

        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return filecontent;
    }


    private static int cond = 0; // 1-online , 0-qa

    public static int pushMsg(String emailAddress, String title, List<JSONObject> attachements){
        String url = "http://mailservice.qc.huohua.cn/sendmail";
        if (cond == 0)
            url = "http://mailservice.qa.huohua.cn/sendmail";
        JSONObject jsonObject = new JSONObject();
        String uuid = UUID.randomUUID().toString();
        jsonObject.put("traceId",uuid);
        jsonObject.put("title",title);
        jsonObject.put("content",String.format("这是一个附件邮件,有%s个文件",attachements.size()) );
        jsonObject.put("from","info@huohua.cn");
        jsonObject.put("onceSend",1);
        jsonObject.put("to",emailAddress);
        jsonObject.put("contentType","text/html");

        List<JSONObject> attachementList = new ArrayList<JSONObject>();

        for(JSONObject attachement:attachements){
            String path = attachement.getString("path");
            byte[] filecontent = getFileContent(path);
            String s0 = Base64.getEncoder().encodeToString((filecontent));

            attachement.remove("path");
            attachement.put("content",s0);
            attachement.put("contentType","text/plain");

            attachementList.add(attachement);
        }

        jsonObject.put("attachement",attachementList);

        HttpURLConnection conn = null;
        PrintWriter pw = null ;
        BufferedReader rd = null ;
        StringBuilder sb = new StringBuilder ();
        String line = null ;
        String response = null;
        int retcode = 0;
        try {
            conn = (HttpURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Charset", "UTF-8");
            conn.setRequestProperty("Content-Type","application/json;");
            conn.setRequestProperty("accept","application/json");

            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setReadTimeout(20000);
            conn.setConnectTimeout(20000);
            conn.setUseCaches(false);
            conn.connect();


            OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
            wr.write(jsonObject.toString());
            wr.flush();
            wr.close();

            rd  = new BufferedReader( new InputStreamReader(conn.getInputStream(), "UTF-8"));
            while ((line = rd.readLine()) != null ) {
                sb.append(line);
            }
            response = sb.toString();
        } catch (MalformedURLException e) {
            e.printStackTrace();
            retcode = -1;
        } catch (IOException e) {
            e.printStackTrace();
            retcode = -2;
        }finally{
            try {
                if(pw != null){
                    pw.close();
                }
                if(rd != null){
                    rd.close();
                }
                if(conn != null){
                    conn.disconnect();
                }
            } catch (IOException e) {
                e.printStackTrace();
                retcode = -3;
            }
        }
        LOG.info("send email result ={}",response);
        return retcode;
    }


    public static void main(String[] args) {
        System.out.println("");

        JSONObject jsonObject =new JSONObject();
        jsonObject.put("name","testzhuser11_0.csv");
        jsonObject.put("path","/Users/mayunqi/testzhuser11_0.csv");
        List<JSONObject> list = new ArrayList<JSONObject>();
        list.add(jsonObject);
        pushMsg("mayunqi01@huohua.cn","测试邮件" ,list);

    }
}
