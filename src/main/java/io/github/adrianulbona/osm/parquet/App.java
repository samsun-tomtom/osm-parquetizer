package io.github.adrianulbona.osm.parquet;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.pbf2.v0_6.PbfReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.unmodifiableList;
import static org.openstreetmap.osmosis.core.domain.v0_6.EntityType.Node;
import static org.openstreetmap.osmosis.core.domain.v0_6.EntityType.Relation;


/**
 * Created by adrian.bona on 27/03/16.
 */
public class App {

    public static void main(String[] args) {
        final MultiEntitySinkConfig config = new MultiEntitySinkConfig();
        final CmdLineParser cmdLineParser = new CmdLineParser(config);
        try {
            ExecutorService executor = Executors.newFixedThreadPool(3);
            cmdLineParser.parseArgument(args);
            final PbfReader nodeReader = new PbfReader(config.getSource().toFile(), config.threads);
            final PbfReader wayReader = new PbfReader(config.getSource().toFile(), config.threads);
            final PbfReader relationReader = new PbfReader(config.getSource().toFile(), config.threads);
            final ParquetSink<Entity> nodeSink = new ParquetSink<>(
                    config.getSource(),
                    config.getDestinationFolder(),
                    config.getExcludeMetadata(), Node);
            final ParquetSink<Entity> waySink = new ParquetSink<>(
                    config.getSource(),
                    config.getDestinationFolder(),
                    config.getExcludeMetadata(),
                    EntityType.Way);
            final ParquetSink<Entity> relationSink = new ParquetSink<>(
                    config.getSource(),
                    config.getDestinationFolder(),
                    config.getExcludeMetadata(),
                    Relation);
            nodeSink.addObserver(new EntitySinkObserver(Node));
            waySink.addObserver(new EntitySinkObserver(EntityType.Way));
            relationSink.addObserver(new EntitySinkObserver(Relation));
            nodeReader.setSink(nodeSink);
            wayReader.setSink(waySink);
            relationReader.setSink(relationSink);
            executor.submit(nodeReader);
            executor.submit(wayReader);
            executor.submit(relationReader);
            executor.shutdown();
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            System.out.print("Usage: java -jar osm-parquetizer.jar");
            System.out.println();
        }
    }

    private static class MultiEntitySinkConfig implements MultiEntitySink.Config {

        @Argument(index = 0, metaVar = "pbf-path", usage = "the OSM PBF file to be parquetized", required = true)
        private Path source;

        @Argument(index = 1, metaVar = "output-path", usage = "the directory where to store the Parquet files",
                required = false)
        private Path destinationFolder;

        @Option(name = "--pbf-threads", usage = "if present number of threads for PbfReader")
        private int threads = 1;

        @Option(name = "--exclude-metadata", usage = "if present the metadata will not be parquetized")
        private boolean excludeMetadata = false;

        @Option(name = "--no-nodes", usage = "if present the nodes will be not parquetized")
        private boolean noNodes = false;

        @Option(name = "--no-ways", usage = "if present the ways will be not parquetized")
        private boolean noWays = false;

        @Option(name = "--no-relations", usage = "if present the relations will not be parquetized")
        private boolean noRelations = false;

        @Override
        public boolean getExcludeMetadata() {
            return this.excludeMetadata;
        }

        @Override
        public Path getSource() {
            return this.source;
        }

        @Override
        public Path getDestinationFolder() {
            return this.destinationFolder != null ?
                    this.destinationFolder : useSourcePathAsDestinationFolder();
        }

        @Override
        public List<EntityType> entitiesToBeParquetized() {
            final List<EntityType> entityTypes = new ArrayList<>();
            if (!noNodes) {
                entityTypes.add(Node);
            }
            if (!noWays) {
                entityTypes.add(EntityType.Way);
            }
            if (!noRelations) {
                entityTypes.add(Relation);
            }
            return unmodifiableList(entityTypes);
        }

        private Path useSourcePathAsDestinationFolder() {
            final String fileName = this.source.getFileName().toString();
            int pos = fileName.lastIndexOf('.');
            if (pos > -1) {
                return Paths.get(this.source.toAbsolutePath().getParent().toString(), fileName.substring(0, pos));
            }
            else {
                return this.source.toAbsolutePath().getParent();
            }
        }
    }

    private static class EntitySinkObserver implements ParquetSink.Observer {

        private static final Logger LOGGER = LoggerFactory.getLogger(EntitySinkObserver.class);
        private final AtomicLong totalEntitiesCount;
        private final EntityType type;

        EntitySinkObserver(final EntityType type) {
            this.type = type;
            totalEntitiesCount = new AtomicLong();
        }

        @Override
        public void started() {}

        @Override
        public void processed(Entity entity) {

            if (entity.getType().equals(this.type)) {
                final long count = totalEntitiesCount.incrementAndGet();
                if (count % 100000 == 0) {
                    final String message = String.format("%s Entities processed: %d", this.type, count);
                    LOGGER.info(message);
                }
            }
        }

        @Override
        public void ended() {

            final String message = String.format("Total %s entities processed: %d", this.type,
                    totalEntitiesCount.get());
            LOGGER.info(message);
        }

        @Override
        public EntityType getType() {
            return null;
        }
    }
}
