package com.prifender.des.node;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = "com.prifender")
public class Application implements CommandLineRunner {

    @Autowired
    DataExtractionServiceNode dataExtractionServiceNode;
    
    public static void main(String[] args) throws Exception {
        new SpringApplication(Application.class).run(args);
    }

	@Override
    public void run(String... arg0) throws Exception {
        if (arg0.length > 0) {
            throw new ExitException();
        }

        new Thread(this.dataExtractionServiceNode).start();
    }

    private class ExitException extends RuntimeException implements ExitCodeGenerator {
        private static final long serialVersionUID = 1L;

        public int getExitCode() {
            return 10;
        }

    }
}
