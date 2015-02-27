package edu.snu.reef.flexion.examples;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.flexion.core.DataParser;
import edu.snu.reef.flexion.core.ParseException;
import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.IDataReduceSender;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.io.data.loading.api.DataSet;
import org.apache.reef.io.network.util.Pair;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public final class SimpleCmpTask extends UserComputeTask <List<String>>
        implements IDataBroadcastReceiver<Integer>, IDataReduceSender<Integer> {

    private Integer receivedData = 0;
    private Integer dataToSend = 0;

    @Inject
    private SimpleCmpTask() {
    }

    @Override
    public void run(List<String> data) {
        float increment = 0;
        for (int i = 0; i < 500000; i++) {
            increment += Math.random();
        }
        dataToSend = (int) (receivedData + increment);
    }

    @Override
    public Class<? extends DataParser<List<String>>> getDataParserClass() {
        return SimpleDataParser.class;
    }

    @Override
    public void receiveBroadcastData(Integer data) {
        receivedData = data;
    }

    @Override
    public Integer sendReduceData() {
        return dataToSend;
    }

    @Override
    public Class <? extends Reduce.ReduceFunction<Integer>> getReduceFunctionClass() {
        return DataReduceFunction.class;
    }

}

class DataReduceFunction implements Reduce.ReduceFunction<Integer> {

    @Inject
    public DataReduceFunction() {
    }

    @Override
    public final Integer apply(Iterable<Integer> dataList) {
        Integer sum = 0;
        Integer count = 0;
        for (final Integer data : dataList) {
            sum += data;
            count++;
        }

        return sum/count;
    }
}

class SimpleDataParser implements DataParser<List<String>> {
    private final static Logger LOG = Logger.getLogger(SimpleDataParser.class.getName());

    private final DataSet<LongWritable, Text> dataSet;
    private List<String> result;
    private ParseException parseException;

    @Inject
    public SimpleDataParser(final DataSet<LongWritable, Text> dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public final List<String> get() throws ParseException {
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

        List<String> texts = new LinkedList<String>();

        for (final Pair<LongWritable, Text> keyValue : dataSet) {
            texts.add(keyValue.second.toString());
        }

        result = texts;

    }
}



