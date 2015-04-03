package edu.snu.reef.flexion.core;

import edu.snu.reef.flexion.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.ConfigurationBuilder;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationModuleBuilder;

import java.io.IOException;

public final class FlexionConfiguration extends ConfigurationModuleBuilder {


    public final static Configuration CONF(String[] args) throws IOException {
        return CONF(args, new CommandLine());
    }

    public final static Configuration CONF(String[] args, CommandLine cl) throws IOException {

        cl.registerShortNameOfClass(EvaluatorSize.class);
        cl.registerShortNameOfClass(OnLocal.class);
        cl.registerShortNameOfClass(EvaluatorNum.class);
        cl.registerShortNameOfClass(Timeout.class);
        cl.registerShortNameOfClass(InputDir.class);
        final ConfigurationBuilder cb = cl.getBuilder();
        cl.processCommandLine(args);

        return cb.build();

    }

}
