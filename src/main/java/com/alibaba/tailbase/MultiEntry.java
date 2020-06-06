package com.alibaba.tailbase;

import com.alibaba.tailbase.backendprocess.BackendController;
import com.alibaba.tailbase.backendprocess.CheckSumService;
import com.alibaba.tailbase.clientprocess.ClientProcessData;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

@EnableAutoConfiguration
@ComponentScan(basePackages = "com.alibaba.tailbase")
public class MultiEntry {


    public static void main(String[] args) {
        String port = args[0];
        Utils.port = port;
        if (Utils.isBackendProcess()) {
            BackendController.init();
            CheckSumService.start();
        }
        if (Utils.isClientProcess()) {
            ClientProcessData.init();
        }
//        String port = System.getProperty("server.port", "8080");
        SpringApplication.run(MultiEntry.class,
                "--server.port=" + port
        );

    }



}
