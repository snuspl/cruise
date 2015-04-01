package edu.snu.reef.flexion.examples.ml.algorithms;

import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public final class ClusteringDataParser implements DataParser<Pair<List<Vector>, List<Vector>>> {
    private final static Logger LOG = Logger.getLogger(ClusteringDataParser.class.getName());

    private final DataSet<LongWritable, Text> dataSet;
    private Pair<List<Vector>, List<Vector>> result;
    private ParseException parseException;

    @Inject
    public ClusteringDataParser(final DataSet<LongWritable, Text> dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public final Pair<List<Vector>, List<Vector>> get() throws ParseException {
        if (result == null) {
            parse();
        }

        if (parseException != null) {
            throw parseException;
        }

        return result;
    }

    @Override
    public final void parse() {
        List<Vector> centroids = new ArrayList<>();
        List<Vector> points = new ArrayList<>();

        for (final Pair<LongWritable, Text> keyValue : dataSet) {
            String[] split = keyValue.second.toString().trim().split("\\s+");
            if (split.length == 0) {
                continue;
            }

            if (split[0].equals("*")) {
                final Vector centroid = new DenseVector(split.length - 1);
                try {
                    for (int i = 1; i < split.length; i++) {
                        centroid.set(i - 1, Double.valueOf(split[i]));
                    }
                    centroids.add(centroid);

                } catch (final NumberFormatException e) {
                    parseException = new ParseException("Parse failed: numbers should be DOUBLE");
                    return;
                }

            } else {
                final Vector data = new DenseVector(split.length);
                try {
                    for (int i = 0; i < split.length; i++) {
                        data.set(i, Double.valueOf(split[i]));
                    }
                    points.add(data);

                } catch (final NumberFormatException e) {
                    parseException = new ParseException("Parse failed: numbers should be DOUBLE");
                    return;
                }
            }

            result = new Pair<>(centroids, points);
        }
    }
}
