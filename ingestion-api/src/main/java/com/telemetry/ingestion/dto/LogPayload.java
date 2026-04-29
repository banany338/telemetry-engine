package com.telemetry.ingestion.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;

public record LogPayload(
    @NotNull
    @NotEmpty
    @Valid
    List<LogEntry> logs
) {}
