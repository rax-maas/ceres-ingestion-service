package com.rackspacecloud.metrics.ingestionservice.rawlisteners.deserializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Map;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroDeserializer.class);

    protected final Class<T> targetType;

    public AvroDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) { }

    @Override
    public T deserialize(String topicName, byte[] data) {
        if(data == null) return null;

        String stringifiedData = new String(data);
        LOGGER.debug("Data is [{}]", stringifiedData);

        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            Schema schema = targetType.newInstance().getSchema();
            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, inputStream);

            T result = (T) datumReader.read(null, decoder);
            LOGGER.debug("Deserialized data: [{}]", result);

            return result;
        } catch (Exception e) {
            String errorMessage = String.format("Deserialization failed for topic [%s] with exception message: [%s]",
                    topicName, e.getMessage());
            LOGGER.error("{} Data in question is [{}]", errorMessage, stringifiedData);
            throw new SerializationException(errorMessage, e);
        }
    }

    @Override
    public void close() { }
}
