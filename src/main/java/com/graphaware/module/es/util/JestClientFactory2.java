package com.graphaware.module.es.util;

import io.searchbox.client.JestClientFactory;
import org.apache.http.impl.client.HttpClientBuilder;

public class JestClientFactory2 extends JestClientFactory {

    @Override
    protected HttpClientBuilder configureHttpClient(HttpClientBuilder builder) {
        return builder
                .setConnectionManagerShared(true);
    }
}
