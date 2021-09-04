package org.apache.doris.connectors.base.cfg;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class DorisConfigurationTest {
    @Test
    public void testBuildFromMap() throws Exception {
        Map<String, String> map = new HashMap<>();
        map.put(ConfigurationOptions.DORIS_FENODES, "192.168.0.103:8530");
        map.put(ConfigurationOptions.DORIS_USER, "root");
        map.put(ConfigurationOptions.DORIS_PASSWORD, "");
        map.put(ConfigurationOptions.DORIS_TABLE_IDENTIFIER, "poc.dwd_tf_order");
        map.put(ConfigurationOptions.DORIS_READ_FIELD, "col1,col2");
        map.put(ConfigurationOptions.DORIS_REQUEST_QUERY_TIMEOUT_S, "60000");
        map.put(ConfigurationOptions.DORIS_DESERIALIZE_ARROW_ASYNC, "true");
        map.put(ConfigurationOptions.DORIS_EXEC_MEM_LIMIT, "10000000000");
        map.put(ConfigurationOptions.DORIS_REQUEST_BATCH_SIZE, "10000");
        map.put("doris.test.non", "doris");
        DorisConfiguration dorisConfig = DorisConfiguration.buildFromMap(map);
        Assert.assertEquals(dorisConfig.getReadFields(), "col1,col2");
        Assert.assertEquals(dorisConfig.getRequestBatchSize(), 10000);
    }

    @Test
    public void testAuthEncoding() {
        String user = "root";
        String passwd = "";
        String authEncoding = Base64.getEncoder().encodeToString(
                String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(authEncoding, "cm9vdDo=");
    }
}
