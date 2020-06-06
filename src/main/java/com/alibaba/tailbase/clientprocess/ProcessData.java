package com.alibaba.tailbase.clientprocess;

import com.alibaba.tailbase.CommonController;
import com.alibaba.tailbase.Constants;
import com.alibaba.tailbase.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessData implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientProcessData.class.getName());

    // an list of trace map,like ring buffer.  key is traceId, value is spans ,  r
    private static List<Map<String, List<String>>> BATCH_TRACE_LIST = new ArrayList<>();
    // make 50 bucket to cache traceData
    private static int BATCH_COUNT = 15;
    public static  void init() {
        for (int i = 0; i < BATCH_COUNT; i++) {
            BATCH_TRACE_LIST.add(new ConcurrentHashMap<>(Constants.BATCH_SIZE));
        }
    }

    @Override
    public void run() {
        try {
            String path = getPath();
            // process data on client, not server
            if (StringUtils.isEmpty(path)) {
                LOGGER.warn("path is empty");
                return;
            }
            URL url = new URL(path);
            LOGGER.info("data path:" + path);
            HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection(Proxy.NO_PROXY);
            InputStream input = httpConnection.getInputStream();



        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private String getPath(){
        String port = System.getProperty("server.port", "8080");
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
