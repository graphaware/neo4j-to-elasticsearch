package com.graphaware.module.es.util;

import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.apache.http.impl.client.HttpClientBuilder;

import static com.graphaware.module.es.ElasticSearchConfiguration.DEFAULT_CONNECTION_TIMEOUT;
import static com.graphaware.module.es.ElasticSearchConfiguration.DEFAULT_READ_TIMEOUT;

import java.io.IOException;

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

    /**
     * Serialize non-finite doubles (-Infinity, +Infinity, NaN) as strings.
     */
    private class NonFiniteAsStringAdapter extends TypeAdapter<Double> {
        @Override
        public void write(JsonWriter out, Double value) throws IOException {
            if (Double.isFinite(value)) {
                out.value(value);
            } else {
                // serialize as "+Infinity", "-Infinity" and "NaN" (string)
                out.value(value + "");
            }
        }

        @Override
        public Double read(JsonReader in) throws IOException {
            return in.nextDouble();
        }
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
                .gson(new GsonBuilder()
                        .registerTypeAdapter(Double.class, new NonFiniteAsStringAdapter())
                        .create()
                )
                .readTimeout(20000) // 20s
                .connTimeout(10000) // 10s
                .build()
        );
    }
}
