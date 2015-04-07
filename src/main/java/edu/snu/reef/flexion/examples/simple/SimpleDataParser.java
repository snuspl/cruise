package edu.snu.reef.flexion.examples.simple;

import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class SimpleDataParser implements DataParser<List<String>> {
    private final static Logger LOG = Logger.getLogger(SimpleDataParser.class.getName());

    private final DataSet<LongWritable, Text> dataSet;
    private List<String> result;

    @Inject
    public SimpleDataParser(final DataSet<LongWritable, Text> dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public final List<String> get() throws ParseException {
        if (result == null) {
            parse();
        }

        return result;
    }

    @Override
    public final void parse() {

        final List<String> texts = new LinkedList<>();

        for (final Pair<LongWritable, Text> keyValue : dataSet) {
            texts.add(keyValue.second.toString());
        }

        result = texts;

    }
}
