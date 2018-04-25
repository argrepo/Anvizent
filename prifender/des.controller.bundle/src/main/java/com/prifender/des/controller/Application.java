package com.prifender.des.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;

import springfox.documentation.swagger2.annotations.EnableSwagger2;

@SpringBootApplication(exclude = {MongoAutoConfiguration.class})
@EnableSwagger2
@ComponentScan(basePackages = "com.prifender")
public class Application implements CommandLineRunner {
	
    @Autowired
	WorkQueueMonitoring workQueueMonitoring;
    
    @Override
    public void run(String... arg0) throws Exception {
        if (arg0.length > 0 && arg0[0].equals("exitcode")) {
            throw new ExitException();
        }
        new Thread(this.workQueueMonitoring).start();
    }

    public static void main(String[] args) throws Exception {
        new SpringApplication(Application.class).run(args);
    }

    class ExitException extends RuntimeException implements ExitCodeGenerator {
        private static final long serialVersionUID = 1L;

        @Override
        public int getExitCode() {
            return 10;
        }

    }
}
