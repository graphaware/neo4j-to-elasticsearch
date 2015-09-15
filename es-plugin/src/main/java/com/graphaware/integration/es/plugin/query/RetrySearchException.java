package com.graphaware.integration.es.plugin.query;

import java.util.Map;

public class RetrySearchException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private QueryRewriter rewriter;

    public RetrySearchException(QueryRewriter rewriter) {
        super();
        this.rewriter = rewriter;
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return null;
    }

    public Map<String, Object> rewrite(Map<String, Object> source) {
        return rewriter.rewrite(source);
    }

    public interface QueryRewriter {
        Map<String, Object> rewrite(Map<String, Object> source);
    }
}
