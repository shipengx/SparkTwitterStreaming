package com.shipeng;
import java.io.*;

public class Tweet implements Serializable {

    public String status;
    public Tweet() {
    }    

    public String getStatus() {
        return this.status;
    }

    public void setStatus(String value) {
        this.status = value;
    }
}
