package com.telemetry.ingestion.service;

import com.telemetry.ingestion.dto.LogEntry;
import com.telemetry.ingestion.repository.LogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class LogService {

    private static final Logger logger = LoggerFactory.getLogger(LogService.class);
    private final LogRepository logRepository;

    public LogService(LogRepository logRepository) {
        this.logRepository = logRepository;
    }

    @Async
    public void processLogs(List<LogEntry> logs) {
        try {
            logRepository.batchInsert(logs);
            logger.debug("Successfully ingested {} logs in background.", logs.size());
        } catch (Exception e) {
            logger.error("Failed to ingest batch of logs", e);
        }
    }
}
