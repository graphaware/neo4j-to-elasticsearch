package com.graphaware.module.es.util;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.impl.client.HttpClientBuilder;

import static com.graphaware.module.es.ElasticSearchConfiguration.DEFAULT_CONNECTION_TIMEOUT;
import static com.graphaware.module.es.ElasticSearchConfiguration.DEFAULT_READ_TIMEOUT;

/**
 * Customizations:
 * - connectionManagerShared: true
 * - readTimeout: 20s
 * - connTimeout: 10s
 */
public class CustomJestClientFactory extends JestClientFactory {

    private int readTimeout;
    private int connectionTimeout;

    public CustomJestClientFactory() {
    }

    public CustomJestClientFactory(int readTimeout, int connectionTimeout) {
        this.readTimeout = readTimeout;
        this.connectionTimeout = connectionTimeout;
    }

    @Override
    protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
        return builder
                .setConnectionManagerShared(true);
    }

    @Override
    public void setHttpClientConfig(HttpClientConfig httpClientConfig) {
        super.setHttpClientConfig(new HttpClientConfig.Builder(httpClientConfig)
                .readTimeout(readTimeout > 0 ? readTimeout : DEFAULT_READ_TIMEOUT)
                .connTimeout(connectionTimeout > 0 ? connectionTimeout : DEFAULT_CONNECTION_TIMEOUT)
                .build()
        );
    }
}
