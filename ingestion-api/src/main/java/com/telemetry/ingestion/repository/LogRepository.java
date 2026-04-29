package com.telemetry.ingestion.repository;

import com.telemetry.ingestion.dto.LogEntry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

@Repository
public class LogRepository {

    private final JdbcTemplate jdbcTemplate;

    public LogRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void batchInsert(List<LogEntry> logs) {
        String sql = "INSERT INTO raw_logs (timestamp, service_name, endpoint, status_code, response_time_ms) VALUES (?, ?, ?, ?, ?)";
        
        jdbcTemplate.batchUpdate(sql, logs, 1000, (PreparedStatement ps, LogEntry log) -> {
            ps.setTimestamp(1, Timestamp.from(log.timestamp().toInstant()));
            ps.setString(2, log.serviceName());
            
            if (log.endpoint() != null) {
                ps.setString(3, log.endpoint());
            } else {
                ps.setNull(3, java.sql.Types.VARCHAR);
            }
            
            if (log.statusCode() != null) {
                ps.setInt(4, log.statusCode());
            } else {
                ps.setNull(4, java.sql.Types.INTEGER);
            }
            
            if (log.responseTimeMs() != null) {
                ps.setInt(5, log.responseTimeMs());
            } else {
                ps.setNull(5, java.sql.Types.INTEGER);
            }
        });
    }
}
