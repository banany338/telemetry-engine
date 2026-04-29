package com.telemetry.ingestion;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
@EnableAsync
public class IngestionApiApplication {

	public static void main(String[] args) {
		SpringApplication.run(IngestionApiApplication.class, args);
	}

}
