package com.rackspacecloud.metrics.ingestionservice.listeners.rawlisteners.deserializer;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

@Slf4j
public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    protected final Class<T> targetType;

    public AvroDeserializer(Class<T> targetType) {
        this.targetType = targetType;
    }

    @Override
    public void configure(Map<String, ?> map, boolean b) { }

    @Override
    public T deserialize(String topicName, byte[] data) {
        if(data == null) return null;

        log.debug("Data is [{}]", new String(data));

        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            Schema schema = targetType.newInstance().getSchema();
            DatumReader<GenericRecord> datumReader = new SpecificDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, inputStream);

            T result = (T) datumReader.read(null, decoder);
            log.debug("Deserialized data: [{}]", result);

            return result;
        } catch (IOException | InstantiationException | IllegalAccessException e) {
            String errorMessage = String.format("Deserialization failed for topic [%s] with exception message: [%s]",
                    topicName, e.getMessage());
            log.error("{} Data in question is [{}]", errorMessage, new String(data));
            throw new SerializationException(errorMessage, e);
        }
    }

    @Override
    public void close() { }
}
