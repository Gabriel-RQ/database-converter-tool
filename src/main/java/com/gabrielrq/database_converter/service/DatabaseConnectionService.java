package com.gabrielrq.database_converter.service;

import com.gabrielrq.database_converter.dto.DbConnectionConfigDTO;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Service
public class DatabaseConnectionService {

    public DataSource createDataSource(DbConnectionConfigDTO config) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(config.jdbcUrl());
        dataSource.setUsername(config.username());
        dataSource.setPassword(config.password());
        dataSource.setDriverClassName(config.driverClassName());
        return dataSource;
    }

    public JdbcTemplate createJdbcTemplate(DbConnectionConfigDTO config) {
        DataSource dataSource = createDataSource(config);
        return new JdbcTemplate(dataSource);
    }

    public Connection createConnection(DbConnectionConfigDTO config) throws SQLException {
        Connection connection = createDataSource(config).getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

}
