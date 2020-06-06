package com.alibaba.tailbase.clientprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public class ClientProcessData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    // an list of trace map,like ring buffe.  key is traceId, value is spans ,  r
    private static List<Map<String,List<String>>> BATCH_TRACE_LIST = new ArrayList<>();
    // make 50 bucket to cache traceData
    private static int BATCH_COUNT = 15;
    public static  void init() {
        for (int i = 0; i < BATCH_COUNT; i++) {
            BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
        }
    }

    private static int inputPos = 0;

    private static final Object lock = new Object();

    public static void start() {
        Thread t = new Thread(new ClientProcessData(), "ProcessDataThread");
        t.start();
    }

    @Override
    public void run() {
        LOGGER.info("CLIENT PROCESS START.");
            try {
                String path = getPath();
                // process data on client, not server
                if (StringUtils.isEmpty(path)) {
                    LOGGER.warn("path is empty");
                    return;
                }
//            URL url = new URL(path);
                LOGGER.info("data path:" + path);
                BufferedReader bf = Files.newBufferedReader(Paths.get(path));
//            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
//            InputStream input = httpConnection.getInputStream();
//            BufferedReader bf = new BufferedReader(new InputStreamReader(input));
                String line;
                long count = 0;
                Set<String> badTraceIdList = new HashSet<>(1000);
                Map<String, List<String>> traceMap = BATCH_TRACE_LIST.get(inputPos);
                while ((line = bf.readLine()) != null) {
                    count++;
                    String[] cols = line.split("\\|");
                    if (cols != null && cols.length > 1 ) {
                        String traceId = cols[0];
                        List<String> spanList = traceMap.get(traceId);
                        if (null == spanList) {
                            spanList = new ArrayList<>();
                            traceMap.put(traceId, spanList);
                        }
                        spanList.add(line);
                        if (cols.length > 8) {
                            String tags = cols[8];
                            if (tags != null) {
                                if (tags.contains("error=1")) {
                                    badTraceIdList.add(traceId);
                                } else if (tags.contains("http.status_code=") && !tags.contains("http.status_code=200")) {
                                    badTraceIdList.add(traceId);
                                }
                            }
                        }
                    }
                    if (count % Constants.BATCH_SIZE == 0) {
                        synchronized (lock) {
                            // batchPos begin from 0, so need to minus 1
                            int batchPos = (int) count / Constants.BATCH_SIZE - 1;
                            long time = System.currentTimeMillis();
                            System.out.println("通知backend处理一个batch: " + batchPos + ",开始时间: " + time);
                            updateWrongTraceId(badTraceIdList, batchPos);
                            time = System.currentTimeMillis();
                            System.out.println("batch" + batchPos + "通知完成时间: " + time);
                            badTraceIdList.clear();

                            inputPos++;
                            // loop cycle
                            if (inputPos >= BATCH_COUNT) {
                                inputPos = 0;
                            }
                            traceMap = BATCH_TRACE_LIST.get(inputPos);
                            LOGGER.info("suc to updateBadTraceId, batchPos:" + batchPos);
                            // donot produce data, wait backend to consume data
                            // TODO to use lock/notify
                            if (traceMap.size() > 0) {
                                // 使用lock / notice
                                LOGGER.info("Child process is waiting.");
                                lock.wait();
                            }
                        }
                    }
                }
                updateWrongTraceId(badTraceIdList, (int) (count / Constants.BATCH_SIZE - 1));
                bf.close();
//            input.close();
                callFinish();
            } catch (Exception e) {
                LOGGER.warn("fail to process data", e);
            }
        }

    /**
     *  call backend controller to update wrong tradeId list.
     * @param badTraceIdList
     * @param batchPos
     */
    private void updateWrongTraceId(Set<String> badTraceIdList, int batchPos) {
        String json = JSON.toJSONString(badTraceIdList);
        if (badTraceIdList.size() > 0) {
            try {
//                LOGGER.info("updateBadTraceId, json:" + json + ", batch:" + batchPos);
                RequestBody body = new FormBody.Builder()
                        .add("traceIdListJson", json).add("batchPos", batchPos + "").build();
                Request request = new Request.Builder().url("http://localhost:8002/setWrongTraceId").post(body).build();
                Response response = Utils.callHttp(request);
                response.close();
            } catch (Exception e) {
                LOGGER.warn("fail to updateBadTraceId, json:" + json + ", batch:" + batchPos);
            }
        }
    }

    // notify backend process when client process has finished.
    private void callFinish() {
        try {
            Request request = new Request.Builder().url("http://localhost:8002/finish").build();
            Response response = Utils.callHttp(request);
            response.close();
        } catch (Exception e) {
            LOGGER.warn("fail to callFinish");
        }
    }


    public static String getWrongTracing(String wrongTraceIdList, int batchPos) {
        long time = System.currentTimeMillis();
        System.out.println("同步一个batch: " + batchPos + ",开始时间: " + time);
        synchronized (lock) {
            try {
//                LOGGER.info(String.format("getWrongTracing, batchPos:%d, wrongTraceIdList:\n %s" ,
//                        batchPos, wrongTraceIdList));        long time = System.currentTimeMillis();
                time = System.currentTimeMillis();
                System.out.println("同步一个batch: " + batchPos + ", 锁获取到时间: " + time);

                List<String> traceIdList = JSON.parseObject(wrongTraceIdList, new TypeReference<List<String>>(){});
                Map<String,List<String>> wrongTraceMap = new HashMap<>();
                int pos = batchPos % BATCH_COUNT;
                int previous = pos - 1;
                if (previous == -1) {
                    previous = BATCH_COUNT -1;
                }
                int next = pos + 1;
                if (next == BATCH_COUNT) {
                    next = 0;
                }
                getWrongTraceWithBatch(previous, pos, traceIdList, wrongTraceMap);
                getWrongTraceWithBatch(pos, pos, traceIdList,  wrongTraceMap);
                getWrongTraceWithBatch(next, pos, traceIdList, wrongTraceMap);
                // to clear spans, don't block client process thread. TODO to use lock/notify
                BATCH_TRACE_LIST.get(previous).clear();

                System.out.println("同步完一个batch: " + batchPos + ",结束时间: " + time);
                // 释放锁
                if (inputPos == previous) {
                    LOGGER.info("清理完缓存， 释放处理线程锁");
                    lock.notify();
                }


                return JSON.toJSONString(wrongTraceMap);
            } catch (Exception e) {
                LOGGER.error("getWrongTrace wrong", e);
                lock.notify();
                return "";
            }
        }
    }

    private static void getWrongTraceWithBatch(int batchPos, int pos,  List<String> traceIdList, Map<String,List<String>> wrongTraceMap) {
        // donot lock traceMap,  traceMap may be clear anytime.
        Map<String, List<String>> traceMap = BATCH_TRACE_LIST.get(batchPos);
        for (String traceId : traceIdList) {
            List<String> spanList = traceMap.get(traceId);
            if (spanList != null) {
                // one trace may cross to batch (e.g batch size 20000, span1 in line 19999, span2 in line 20001)
                List<String> existSpanList = wrongTraceMap.get(traceId);
                if (existSpanList != null) {
                    existSpanList.addAll(spanList);
                } else {
                    wrongTraceMap.put(traceId, spanList);
                }
                // output spanlist to check
//                String spanListString = spanList.stream().collect(Collectors.joining("\n"));
//                LOGGER.info(String.format("getWrongTracing, batchPos:%d, pos:%d, traceId:%s, spanList:\n %s",
//                        batchPos, pos,  traceId, spanListString));
            }
        }
    }

    private String getPath(){
        String port = Utils.port;
//        String port = System.getProperty("server.port", "8080");
        if ("8000".equals(port)) {
            return "trace1.data";
//            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace1.data";
        } else if ("8001".equals(port)){
            return "trace2.data";
//            return "http://localhost:" + CommonController.getDataSourcePort() + "/trace2.data";
        } else {
            return null;
        }


    }

}
