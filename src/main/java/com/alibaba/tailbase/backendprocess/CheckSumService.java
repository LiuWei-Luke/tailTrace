package com.alibaba.tailbase.backendprocess;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Utils;
import com.alibaba.tailbase.clientprocess.ClientProcessData;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.tailbase.Constants.CLIENT_PROCESS_PORT1;
import static com.alibaba.tailbase.Constants.CLIENT_PROCESS_PORT2;

public class CheckSumService implements Runnable{


    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    // save chuckSum for the total wrong trace
    private static Map<String, String> TRACE_CHUCKSUM_MAP= new ConcurrentHashMap<>();

    public static void start() {
        Thread t = new Thread(new CheckSumService(), "CheckSumServiceThread");
        t.start();
    }

    @Override
    public void run() {
        TraceIdBatch traceIdBatch = null;
//        String[] ports = new String[]{CLIENT_PROCESS_PORT1, CLIENT_PROCESS_PORT2};
        List<String> ports = new ArrayList<>();
        ports.add(CLIENT_PROCESS_PORT1);
        ports.add(CLIENT_PROCESS_PORT2);
        while (true) {
            try {
                traceIdBatch = BackendController.getFinishedBatch();
                if (traceIdBatch == null) {
                    // send checksum when client process has all finished.
                    if (BackendController.isFinished()) {
                        if (sendCheckSum()) {
                            break;
                        }
                    }
                    continue;
                }
                Map<String, Set<String>> map = new ConcurrentHashMap<>();
                // if (traceIdBatch.getTraceIdList().size() > 0) {
                int batchPos = traceIdBatch.getBatchPos();
                List<String> traceIds = traceIdBatch.getTraceIdList();
                // to get all spans from remote
                long time = System.currentTimeMillis();
                LOGGER.info("Parallel handle batch at: {}", time);
                ports.parallelStream().forEach(port -> {
                    Map<String, List<String>> processMap =
                            getWrongTrace(JSON.toJSONString(traceIds), port, batchPos);
                    if (processMap != null) {
                        for (Map.Entry<String, List<String>> entry : processMap.entrySet()) {
                            String traceId = entry.getKey();
                            Set<String> spanSet = map.get(traceId);
                            if (spanSet == null) {
                                spanSet = new HashSet<>();
                                map.put(traceId, spanSet);
                            }
                            spanSet.addAll(entry.getValue());
                        }
                    }
                });


                time = System.currentTimeMillis();
                LOGGER.info("End handle batch at: {}", time);
//                    LOGGER.info("getWrong:" + batchPos + ", traceIdsize:" + traceIdBatch.getTraceIdList().size() + ",result:" + map.size());
               // }
                time = System.currentTimeMillis();
                System.out.println("排序wrong trace batch: " + batchPos + ",开始: " + time);
                for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
                    String traceId = entry.getKey();
                    Set<String> spanSet = entry.getValue();
                    // order span with startTime
                    String spans = spanSet.stream().sorted(
                            Comparator.comparing(CheckSumService::getStartTime)).collect(Collectors.joining("\n"));
                    spans = spans + "\n";
                    // output all span to check
                   // LOGGER.info("traceId:" + traceId + ",value:\n" + spans);
                    TRACE_CHUCKSUM_MAP.put(traceId, Utils.MD5(spans));
                }
                time = System.currentTimeMillis();
                System.out.println("排序wrong trace batch: " + batchPos + ",结束: " + time);
            } catch (Exception e) {
                // record batchPos when an exception  occurs.
                int batchPos = 0;
                if (traceIdBatch != null) {
                    batchPos = traceIdBatch.getBatchPos();
                }
                LOGGER.warn(String.format("fail to getWrongTrace, batchPos:%d", batchPos), e);
            } finally {
                if (traceIdBatch == null) {
                    try {
                        Thread.sleep(100);
                    } catch (Throwable e) {
                        // quiet
                    }
                }
            }
        }
    }

    /**
     * call client process, to get all spans of wrong traces.
     * @param traceIdList
     * @param port
     * @param batchPos
     * @return
     */
    private Map<String,List<String>>  getWrongTrace(@RequestParam String traceIdList, String port, int batchPos) {
        try {
            RequestBody body = new FormBody.Builder()
                    .add("traceIdList", traceIdList).add("batchPos", batchPos + "").build();
            String url = String.format("http://localhost:%s/getWrongTrace", port);
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            Map<String,List<String>> resultMap = JSON.parseObject(response.body().string(),
                    new TypeReference<Map<String, List<String>>>() {});
            response.close();
            return resultMap;
        } catch (Exception e) {
            LOGGER.warn("fail to getWrongTrace, json:" + traceIdList + ",batchPos:" + batchPos, e);
        }
        return null;
    }


    private boolean sendCheckSum() {
        try {
            String result = JSON.toJSONString(TRACE_CHUCKSUM_MAP);
            RequestBody body = new FormBody.Builder()
                    .add("result", result).build();
            String url = String.format("http://0.0.0.0:%s/api/finished", CommonController.getDataSourcePort());
            Request request = new Request.Builder().url(url).post(body).build();
            Response response = Utils.callHttp(request);
            if (response.isSuccessful()) {
                response.close();
//                LOGGER.warn("suc to sendCheckSum, result:" + result);
                return true;
            }
            LOGGER.warn("fail to sendCheckSum:" + response.message());
            response.close();
            return false;
        } catch (Exception e) {
            LOGGER.warn("fail to call finish", e);
        }
        return false;
    }

    public static long getStartTime(String span) {
        if (span != null) {
            String[] cols = span.split("\\|");
            if (cols.length > 8) {
                return Utils.toLong(cols[1], -1);
            }
        }
        return -1;
    }

}
