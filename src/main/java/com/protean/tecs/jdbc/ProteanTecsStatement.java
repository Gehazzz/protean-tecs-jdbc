package com.protean.tecs.jdbc;

import com.protean.tecs.jdbc.client.AnalysisServiceClient;
import com.protean.tecs.jdbc.dto.ResultSetData;
import com.protean.tecs.jdbc.operation.ResultSetMapping;
import com.protean.tecs.jdbc.operation.ResultSetMappingToResultSetData;
import com.sun.rowset.CachedRowSetImpl;
import lombok.SneakyThrows;

import javax.sql.rowset.CachedRowSet;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;

public class ProteanTecsStatement implements Statement {
    private final Statement statement;
    private final ProteanTecsJdbcConnection jdbcConnection;
    private final AnalysisServiceClient client;
    private final ResultSetMapping<ResultSetData> resultSetMapping;

    public ProteanTecsStatement(Statement statement, ProteanTecsJdbcConnection jdbcConnection, AnalysisServiceClient client) {
        this.statement = statement;
        this.jdbcConnection = jdbcConnection;
        this.client = client;
        this.resultSetMapping = new ResultSetMappingToResultSetData();
    }


    @Override
    @SneakyThrows
    public ResultSet executeQuery(String sql) throws SQLException {
        client.sendLog(sql);
        return statement.executeQuery(sql);
    }

    @Override
    @SneakyThrows
    public int executeUpdate(String sql) throws SQLException {
        client.sendLog(sql);
        return statement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        statement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return statement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        statement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return statement.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        statement.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        statement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return statement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        statement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        statement.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        statement.setCursorName(name);
    }

    @Override
    @SneakyThrows
    public boolean execute(String sql) throws SQLException {
        Instant start = Instant.now();
        boolean execute = statement.execute(sql);
        Instant end = Instant.now();
        long executionTimeMs = Duration.between(start, end).toMillis();
        String message = "Query: " + sql + ", Execution time: " + executionTimeMs + " ms.";
        client.sendLog(message);
        return execute;
    }

   /* @Override
    @SneakyThrows
    public boolean execute(String sql) throws SQLException {
        Instant start = Instant.now();
        boolean execute = statement.execute(sql);
        Instant end = Instant.now();
        long executionTimeMs = Duration.between(start, end).toMillis();
        String message = "Query: " + sql + ", Execution time: " + executionTimeMs;
        client.sendLog(message);
        return execute;
    }*/

    @SneakyThrows
    @Override
    public ResultSet getResultSet() throws SQLException {
        ResultSet resultSet = statement.getResultSet();
        CachedRowSet cachedRowSet = new CachedRowSetImpl();
        cachedRowSet.populate(resultSet);
        ResultSetData resultSetData = resultSetMapping.resultSetToDto(cachedRowSet);
        client.sendLog(resultSetData.toString());
        return cachedRowSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return statement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return statement.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        statement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return statement.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        statement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return statement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return statement.getResultSetType();
    }

    @Override
    @SneakyThrows
    public void addBatch(String sql) throws SQLException {
        client.sendLog(sql);
        statement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        statement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return this.jdbcConnection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return statement.getMoreResults(current);
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return statement.getGeneratedKeys();
    }

    @Override
    @SneakyThrows
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        client.sendLog(sql);
        return statement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    @SneakyThrows
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        client.sendLog(sql);
        return statement.executeUpdate(sql, columnIndexes);
    }

    @Override
    @SneakyThrows
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        client.sendLog(sql);
        return statement.executeUpdate(sql, columnNames);
    }

    @Override
    @SneakyThrows
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        client.sendLog(sql);
        return statement.execute(sql, autoGeneratedKeys);
    }

    @Override
    @SneakyThrows
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        client.sendLog(sql);
        return statement.execute(sql, columnIndexes);
    }

    @Override
    @SneakyThrows
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        client.sendLog(sql);
        return statement.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return statement.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        statement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return statement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        statement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return statement.isCloseOnCompletion();
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return statement.getLargeUpdateCount();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        statement.setLargeMaxRows(max);
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        return statement.getLargeMaxRows();
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        return statement.executeLargeBatch();
    }

    @Override
    @SneakyThrows
    public long executeLargeUpdate(String sql) throws SQLException {
        client.sendLog(sql);
        return statement.executeLargeUpdate(sql);
    }

    @Override
    @SneakyThrows
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        client.sendLog(sql);
        return statement.executeLargeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return statement.executeLargeUpdate(sql, columnIndexes);
    }

    @Override
    @SneakyThrows
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        client.sendLog(sql);
        return statement.executeLargeUpdate(sql, columnNames);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return statement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return statement.isWrapperFor(iface);
    }
}
