package com.graphaware.module.es.proc.result;

public class SingleResult {

    public Object result;

    public SingleResult(Object result) {
        this.result = result;
    }

    public static SingleResult success() {
        return new SingleResult("SUCCESS");
    }

    public static SingleResult failure() {
        return new SingleResult("FAILURE");
    }
}
