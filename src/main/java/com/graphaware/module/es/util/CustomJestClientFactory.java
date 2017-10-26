package com.graphaware.module.es.util;

import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Customizations:
 * - connectionManagerShared: true
 * - readTimeout: 20s
 * - connTimeout: 10s
 */
public class CustomJestClientFactory extends JestClientFactory {

    @Override
    protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
        return builder
                .setConnectionManagerShared(true);
    }

    @Override
    public void setHttpClientConfig(HttpClientConfig httpClientConfig) {
        super.setHttpClientConfig(new HttpClientConfig.Builder(httpClientConfig)
                .readTimeout(20000) // 20s
                .connTimeout(10000) // 10s
                .build()
        );
    }
}
