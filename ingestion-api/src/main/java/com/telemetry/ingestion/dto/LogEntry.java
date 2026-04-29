package com.telemetry.ingestion.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.OffsetDateTime;

public record LogEntry(
    @NotNull
    OffsetDateTime timestamp,
    
    @NotBlank
    String serviceName,
    
    String endpoint,
    
    Integer statusCode,
    
    Integer responseTimeMs
) {}
