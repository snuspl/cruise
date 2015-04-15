package edu.snu.reef.flexion.examples.ml.data;

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

public final class ClusteringDataParser implements DataParser<List<Vector>> {
    private final static Logger LOG = Logger.getLogger(ClusteringDataParser.class.getName());

    private final DataSet<LongWritable, Text> dataSet;
    private List<Vector> result;
    private ParseException parseException;

    @Inject
    public ClusteringDataParser(final DataSet<LongWritable, Text> dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public final List<Vector> get() throws ParseException {
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

        final List<Vector> points = new ArrayList<>();

        for (final Pair<LongWritable, Text> keyValue : dataSet) {

            final String text = keyValue.second.toString().trim();

            if (text.startsWith("#")) {
                continue;
            }

            final String[] split = keyValue.second.toString().trim().split("\\s+");

            if (split.length == 0) {
                continue;
            }

            final Vector point = new DenseVector(split.length);
            try {
                for (int i = 0; i < split.length; i++) {
                    point.set(i, Double.valueOf(split[i]));
                }
                points.add(point);

            } catch (final NumberFormatException e) {
                parseException = new ParseException("Parse failed: each field should be a number");
                return;
            }
        }

        result = points;

    }
}
