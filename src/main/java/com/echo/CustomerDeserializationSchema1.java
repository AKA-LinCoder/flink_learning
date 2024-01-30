package com.echo;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class CustomerDeserializationSchema1 implements DebeziumDeserializationSchema {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
        //创建JSON对象用于封装结果
        JSONObject result = new JSONObject();

        //获取库名表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db", fields[1]);
        result.put("tableName", fields[2]);


        Struct value = (Struct) sourceRecord.value();
        //获取before数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            Schema schema = before.schema();
            ///获取列名
            List<Field> fields1 = schema.fields();
            for (Field field : fields1) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);
        //获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            Schema schema = after.schema();
            ///获取列名
            List<Field> fields1 = schema.fields();
            for (Field field : fields1) {
                afterJson.put(field.name(), before.get(field));
            }
        }
        result.put("after", afterJson);
        //获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);
        //输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
