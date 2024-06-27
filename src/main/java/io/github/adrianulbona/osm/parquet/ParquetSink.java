package io.github.adrianulbona.osm.parquet;

import org.apache.parquet.hadoop.ParquetWriter;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.lang.String.format;


public class ParquetSink<T extends Entity> implements Sink {

    private final Path source;
    private final Path destinationFolder;
    private final boolean excludeMetadata;
    private final EntityType entityType;
    private final List<Predicate<T>> filters;

    private final List<ParquetSink.Observer> observers;

    private ParquetWriter<T> writer;

    public ParquetSink(Path source, Path destinationFolder, boolean excludeMetadata, EntityType entityType) {
        this.source = source;
        this.destinationFolder = Paths.get(destinationFolder.toAbsolutePath().toString(),
                entityType.toString().toLowerCase());
        this.excludeMetadata = excludeMetadata;
        this.entityType = entityType;
        this.filters = new ArrayList<>();
        this.observers = new ArrayList<>();
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
        final String pbfName = source.getFileName().toString();
        final String entityName = entityType.name().toLowerCase();
        final Path destination = destinationFolder.resolve(format("%s.%s.parquet", pbfName, entityName));
        try {
            this.writer = ParquetWriterFactory.buildFor(destination.toAbsolutePath().toString(), excludeMetadata,
                    entityType);
        } catch (IOException e) {
            throw new RuntimeException("Unable to build writers", e);
        }
        this.observers.forEach(ParquetSink.Observer::started);
    }

    @Override
    public void process(EntityContainer entityContainer) {
        try {
            if (this.entityType == entityContainer.getEntity().getType()) {
                final T entity = (T) entityContainer.getEntity();
                if (filters.stream().noneMatch(filter -> filter.test(entity))) {
                    writer.write(entity);
                    this.observers.forEach(o -> o.processed(entity));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to write entity", e);
        }
    }

    @Override
    public void complete() {
        try {
            this.writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Unable to close writers", e);
        }
        this.observers.forEach(ParquetSink.Observer::ended);
    }

    @Override
    public void close() {

    }

    public void addObserver(final ParquetSink.Observer observer) {
        this.observers.add(observer);
    }

    public void removeObserver(final ParquetSink.Observer observer) {
        this.observers.remove(observer);
    }

    public void addFilter(Predicate<T> predicate) {
        this.filters.add(predicate);
    }

    public void removeFilter(Predicate<T> predicate) {
        this.filters.remove(predicate);
    }

    public interface Observer {

        void started();

        void processed(Entity entity);

        void ended();

        EntityType getType();
    }
}
