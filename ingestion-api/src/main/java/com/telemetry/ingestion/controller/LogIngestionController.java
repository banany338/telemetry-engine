package com.telemetry.ingestion.controller;

import com.telemetry.ingestion.dto.LogPayload;
import com.telemetry.ingestion.service.LogService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/logs")
public class LogIngestionController {

    private final LogService logService;

    public LogIngestionController(LogService logService) {
        this.logService = logService;
    }

    @PostMapping
    public ResponseEntity<Void> ingestLogs(@Valid @RequestBody LogPayload payload) {
        logService.processLogs(payload.logs());
        return ResponseEntity.accepted().build();
    }
}
