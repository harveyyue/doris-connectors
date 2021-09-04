package org.apache.doris.connectors.base.rest.models;

/**
 * Doris restful api response class
 *
 * @param <T>
 */
public class DorisResponse<T> {
    private String msg;
    private int code;
    private int count;
    private T data;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
