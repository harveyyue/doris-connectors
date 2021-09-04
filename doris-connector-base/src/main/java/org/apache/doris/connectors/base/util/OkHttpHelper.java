package org.apache.doris.connectors.base.util;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import org.apache.doris.connectors.base.cfg.DorisConfiguration;

import java.util.concurrent.TimeUnit;

public class OkHttpHelper {

    public static final OkHttpClient DEFAULT_OK_HTTP_CLIENT =
            new OkHttpClient
                    .Builder()
                    .connectTimeout(60, TimeUnit.SECONDS)
                    .writeTimeout(60, TimeUnit.SECONDS)
                    .readTimeout(60, TimeUnit.SECONDS)
                    .build();

    public static String getCredentials(DorisConfiguration dorisConfig) {
        return Credentials.basic(dorisConfig.getUsername(), dorisConfig.getPassword());
    }
}
