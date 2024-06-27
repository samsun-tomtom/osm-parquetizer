package io.github.adrianulbona.osm.parquet;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.lifecycle.Closeable;
import org.openstreetmap.osmosis.core.lifecycle.Completable;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;


/**
 * Created by adrian.bona on 27/03/16.
 */
public class MultiEntitySink implements Sink {

    private final List<ParquetSink<Entity>> converters;

    public MultiEntitySink(Config config) {
        final List<EntityType> entityTypes = config.entitiesToBeParquetized();
        this.converters = entityTypes.stream().map(type -> new ParquetSink<>(config.getSource(),
                config.getDestinationFolder(), config.getExcludeMetadata(), type)).collect(toList());
    }

    @Override
    public void process(EntityContainer entityContainer) {
        this.converters.forEach(converter -> converter.process(entityContainer));

    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        this.converters.forEach(converter -> converter.initialize(metaData));
    }

    @Override
    public void complete() {
        this.converters.forEach(Completable::complete);
    }

    @Override
    public void close() {
        this.converters.forEach(Closeable::close);
    }

    public interface Observer {

        void started();

        void processed(Entity entity);

        void ended();
    }

    public interface Config {

        Path getSource();

        Path getDestinationFolder();

        boolean getExcludeMetadata();

        List<EntityType> entitiesToBeParquetized();
    }
}
