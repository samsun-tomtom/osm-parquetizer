package io.github.adrianulbona.osm.parquet;

import io.github.adrianulbona.osm.parquet.convertor.NodeWriteSupport;
import io.github.adrianulbona.osm.parquet.convertor.RelationWriteSupport;
import io.github.adrianulbona.osm.parquet.convertor.WayWriteSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.openstreetmap.osmosis.core.domain.v0_6.*;

import java.io.IOException;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;


/**
 * Created by adrian.bona on 26/03/16.
 */
public class ParquetWriterFactory {

    private static final CompressionCodecName COMPRESSION = SNAPPY;

    public static <T extends Entity> ParquetWriter<T> buildFor(String destination, boolean excludeMetadata,
            EntityType entityType) throws IOException {
        switch (entityType) {
            case Node:
                return (ParquetWriter<T>) NodesWriterBuilder.standard(destination, excludeMetadata);
            case Way:
                return (ParquetWriter<T>) WaysWriterBuilder.standard(destination, excludeMetadata);
            case Relation:
                return (ParquetWriter<T>) RelationsWriterBuilder.standard(destination, excludeMetadata);
            default:
                throw new RuntimeException("Invalid entity type");
        }
    }

    public static class WaysWriterBuilder extends ParquetWriter.Builder<Way, WaysWriterBuilder> {

        private final boolean excludeMetadata;

        protected WaysWriterBuilder(Path file, boolean excludeMetadata) {
            super(file);
            this.excludeMetadata = excludeMetadata;
        }

        @Override
        protected WaysWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Way> getWriteSupport(Configuration conf) {
            return new WayWriteSupport(excludeMetadata);
        }

        public static ParquetWriter<Way> standard(String destination, boolean excludeMetadata) throws IOException {
            return new WaysWriterBuilder(new Path(destination), excludeMetadata).self().withRowGroupSize(16777216)
                    .withCompressionCodec(COMPRESSION).withWriteMode(OVERWRITE).build();
        }
    }


    public static class NodesWriterBuilder extends ParquetWriter.Builder<Node, NodesWriterBuilder> {

        private final boolean excludeMetadata;

        protected NodesWriterBuilder(Path file, boolean excludeMetadata) {
            super(file);
            this.excludeMetadata = excludeMetadata;
        }

        @Override
        protected NodesWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Node> getWriteSupport(Configuration conf) {
            return new NodeWriteSupport(excludeMetadata);
        }

        public static ParquetWriter<Node> standard(String destination, boolean excludeMetadata) throws IOException {
            return new NodesWriterBuilder(new Path(destination), excludeMetadata).self().withRowGroupSize(16777216)
                    .withCompressionCodec(COMPRESSION).withWriteMode(OVERWRITE).build();
        }
    }


    public static class RelationsWriterBuilder extends ParquetWriter.Builder<Relation, RelationsWriterBuilder> {

        private final boolean excludeMetadata;

        protected RelationsWriterBuilder(Path file, boolean excludeMetadata) {
            super(file);
            this.excludeMetadata = excludeMetadata;
        }

        @Override
        protected RelationsWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<Relation> getWriteSupport(Configuration conf) {
            return new RelationWriteSupport(excludeMetadata);
        }

        public static ParquetWriter<Relation> standard(String destination, boolean excludeMetadata) throws IOException {
            return new RelationsWriterBuilder(new Path(destination), excludeMetadata).self().withRowGroupSize(16777216)
                    .withCompressionCodec(COMPRESSION).withWriteMode(OVERWRITE).build();
        }
    }
}
