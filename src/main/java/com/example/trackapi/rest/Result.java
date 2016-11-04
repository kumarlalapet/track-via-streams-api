package com.example.trackapi.rest;

/**
 * Created by mlalapet on 11/02/16.
 */
public class Result {

    private Status status;
    private String id;

    enum Status {
        SUCCESS, FAIL
    }

    public Result(String id, Status status){
        this.status = status;
        this.id = id;
    }

    public Status getStatus() {
        return status;
    }

    public String getId() {
        return id;
    }

}
