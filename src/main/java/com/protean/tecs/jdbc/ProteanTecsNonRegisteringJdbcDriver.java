package com.protean.tecs.jdbc;

import com.protean.tecs.jdbc.client.AnalysisServiceClient;
import okhttp3.*;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.util.Properties;
import java.util.logging.Logger;

public class ProteanTecsNonRegisteringJdbcDriver implements Driver, Closeable {
    //jdbc:proteantecs://localhost:5433/docker
    //dbadmin
    private static final String DRIVER_URL_START = "jdbc:proteantecs:";

    private final String DRIVER_VERSION = "0.0";

    private final OkHttpClient httpClient;

    private final AnalysisServiceClient client;

    ProteanTecsNonRegisteringJdbcDriver() {
        this.httpClient = new OkHttpClient.Builder()
                .addInterceptor(userAgent("ProteanTecsDriver" + "/" + DRIVER_VERSION))
                .socketFactory(new SocketChannelSocketFactory())
                .build();
        this.client = new AnalysisServiceClient(httpClient);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            client.sendLog(info.toString());
            client.sendForAnalysis(url);
            client.sendForAnalysis(info.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (!acceptsURL(url)) return null;
        //String newUrl = url.replace(DRIVER_URL_START, "jdbc:vertica:");
        String newUrl = url.replace(DRIVER_URL_START, "jdbc:mysql:");

        Connection connection = DriverManager.getDriver(newUrl).connect(newUrl, info);
        //DriverManager.getConnection(newUrl, info);
        return new ProteanTecsJdbcConnection(connection, "mysql", client);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(DRIVER_URL_START);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        /*Stream.of("user", "password", "url").map()*/
        /*return info.entrySet().stream().map(entry -> {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            return new DriverPropertyInfo(key, value);
        }).toArray(DriverPropertyInfo[]::new);*/
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 4;
    }

    @Override
    public int getMinorVersion() {
        return 2;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

    @Override
    public void close() throws IOException {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    private Interceptor userAgent(String userAgent) {
        return chain -> chain.proceed(chain.request().newBuilder()
                .header("User-Agent", userAgent)
                .build());
    }
}
