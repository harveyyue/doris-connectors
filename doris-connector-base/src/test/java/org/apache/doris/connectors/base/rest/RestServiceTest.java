package org.apache.doris.connectors.base.rest;

import org.apache.doris.connectors.base.cfg.DorisConfiguration;
import org.apache.doris.connectors.base.exception.DorisException;
import org.apache.doris.connectors.base.rest.models.BackendRow;
import org.apache.doris.connectors.base.rest.models.PartitionDefinition;
import org.apache.doris.connectors.base.rest.models.Schema;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class RestServiceTest {

    @Test
    public void test() throws DorisException {
        DorisConfiguration dorisConfig = new DorisConfiguration.Builder()
                .withFenodes("10.241.1.16:8530")
                .withTableIdentifier("poc.dim_supplier_sink")
                .withUsername("root")
                .withPassword("root")
                .build();

        List<BackendRow> backendRows = RestService.getBackends(dorisConfig);
        List<PartitionDefinition> partitions = RestService.findPartitions(dorisConfig);
        Schema schema = RestService.getSchema(dorisConfig);
        Map<String, Object> ret = RestService.executeSql(dorisConfig, "select * from poc.dim_supplier_sink");
        System.out.printf("");
    }
}
