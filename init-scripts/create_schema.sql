-- Schema for all nodes (homogeneous)
CREATE DATABASE IF NOT EXISTS imdb_distributed;
USE imdb_distributed;

DROP TABLE IF EXISTS titles;

CREATE TABLE titles (
    tconst VARCHAR(20) PRIMARY KEY,
    title_type VARCHAR(20) NOT NULL,
    primary_title VARCHAR(500) NOT NULL,
    start_year INT,
    runtime_minutes INT,
    genres VARCHAR(100),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    INDEX idx_title_type (title_type),
    INDEX idx_year (start_year),
    INDEX idx_runtime (runtime_minutes)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Transaction log table 
CREATE TABLE transaction_log (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    source_node VARCHAR(10) NOT NULL, 
    target_node VARCHAR(10) NOT NULL,
    operation_type ENUM('INSERT', 'UPDATE', 'DELETE') NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    record_id VARCHAR(20) NOT NULL,  -- tconst value
    status ENUM('SUCCESS', 'FAILED', 'PENDING') DEFAULT 'PENDING',
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    
    -- for retry logic
    query_text TEXT,  -- the actual SQL query
    query_params JSON,  -- parameters as JSON array
    error_message TEXT,
    
    -- timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_retry_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    
    INDEX idx_transaction (transaction_id),
    INDEX idx_status (status),
    INDEX idx_target_node (target_node),
    INDEX idx_pending_retries (status, retry_count, target_node)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;