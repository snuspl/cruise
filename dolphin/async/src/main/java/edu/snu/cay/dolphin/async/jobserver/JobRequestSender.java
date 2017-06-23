/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.async.jobserver;

import edu.snu.cay.common.param.Parameters.*;
import edu.snu.cay.dolphin.async.*;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.dolphin.async.optimizer.api.OptimizationOrchestrator;
import edu.snu.cay.dolphin.async.optimizer.impl.DummyOrchestrator;
import edu.snu.cay.services.et.configuration.parameters.KeyCodec;
import edu.snu.cay.services.et.configuration.parameters.UpdateValueCodec;
import edu.snu.cay.services.et.configuration.parameters.ValueCodec;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.reef.annotations.audience.ClientSide;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.CommandLine;
import org.apache.reef.tang.formats.ConfigurationSerializer;
import org.apache.reef.tang.types.NamedParameterNode;

import javax.annotation.Nullable;
import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 * A HTTP request sender to control job server.
 * It submits specific ML job dynamically to running job server by {@link #submitJob}.
 * All parameters related to job are determined by command line.
 * It also closes running job server by {@link #closeJobServer}.
 * Note that it supports only NMF job in this stage.
 */
@ClientSide
public final class JobRequestSender {

  private static final Logger LOG = Logger.getLogger(JobRequestSender.class.getName());

  private JobRequestSender() {

  }

  public static void submitJob(final String appId,
                               final String[] args,
                               final ETDolphinConfiguration dolphinConf) {
    try {
      final List<Configuration> configurations = parseCommandLine(args, dolphinConf.getParameterClassList());
      final Configuration masterParamConf = configurations.get(0);
      final Configuration serverParamConf = configurations.get(1);
      final Configuration workerParamConf = configurations.get(2);
      final Configuration userParamConf = configurations.get(3);
      final Configuration urlConf = configurations.get(4);

      // server conf. servers will be spawned with this configuration
      final Configuration serverConf = Configurations.merge(
          serverParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(UpdateFunction.class, dolphinConf.getModelUpdateFunctionClass())
              .bindNamedParameter(KeyCodec.class, dolphinConf.getModelKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, dolphinConf.getModelValueCodecClass())
              .bindNamedParameter(UpdateValueCodec.class, dolphinConf.getModelUpdateValueCodecClass())
              .build());

      // worker conf. workers will be spawned with this configuration
      final Configuration workerConf = Configurations.merge(
          workerParamConf, userParamConf,
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindImplementation(Trainer.class, dolphinConf.getTrainerClass())
              .bindImplementation(DataParser.class, dolphinConf.getInputParserClass())
              .bindImplementation(TrainingDataProvider.class, ETTrainingDataProvider.class)
              .bindImplementation(ModelAccessor.class, ETModelAccessor.class)
              .bindNamedParameter(KeyCodec.class, dolphinConf.getInputKeyCodecClass())
              .bindNamedParameter(ValueCodec.class, dolphinConf.getInputValueCodecClass())
              .build());

      // job configuration. driver will use this configuration to spawn a job
      final Configuration jobConf = getJobConfiguration(appId, masterParamConf, serverConf, workerConf, userParamConf);

      // send http request
      final ConfigurationSerializer configurationSerializer = new AvroConfigurationSerializer();
      final Injector urlInjector = Tang.Factory.getTang().newInjector(urlConf);
      final String targetAddress = urlInjector.getNamedInstance(HttpAddress.class);
      final String targetPort = urlInjector.getNamedInstance(HttpPort.class);
      sendRequest("submit", targetAddress, targetPort, configurationSerializer.toString(jobConf));

    } catch (IOException | InjectionException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static void closeJobServer(final String[] args) {
    try {
      final CommandLine cl = new CommandLine();
      cl.registerShortNameOfClass(HttpAddress.class);
      cl.registerShortNameOfClass(HttpPort.class);

      // http configuration, target of http request is specified by this configuration.
      final Configuration httpConf;
      httpConf = cl.processCommandLine(args).getBuilder().build();
      final Injector httpParamInjector = Tang.Factory.getTang().newInjector(httpConf);
      final String address = httpParamInjector.getNamedInstance(HttpAddress.class);
      final String port = httpParamInjector.getNamedInstance(HttpPort.class);
      sendRequest("finish", address, port, null);

    } catch (IOException | InjectionException e) {
      throw new RuntimeException(e);
    }
  }

  private static List<Configuration> parseCommandLine(
      final String[] args, final List<Class<? extends Name<?>>> userParamList)
      throws IOException, InjectionException, ClassNotFoundException {

    // parameters for master
    final List<Class<? extends Name<?>>> masterParamList = Arrays.asList(
        MaxNumEpochs.class, MiniBatchSize.class, NumWorkers.class
    );

    // parameters for servers
    final List<Class<? extends Name<?>>> serverParamList = Arrays.asList(
        NumServers.class, ServerMemSize.class, NumServerCores.class,
        NumServerHandlerThreads.class, NumServerSenderThreads.class,
        ServerHandlerQueueSize.class, ServerSenderQueueSize.class,
        NumServerBlocks.class
    );

    // parameters for workers
    final List<Class<? extends Name<?>>> workerParamList = Arrays.asList(
        NumWorkers.class, WorkerMemSize.class, NumWorkerCores.class,
        NumWorkerHandlerThreads.class, NumWorkerSenderThreads.class,
        WorkerHandlerQueueSize.class, WorkerSenderQueueSize.class,
        NumWorkerBlocks.class, NumTrainerThreads.class, MaxNumEpochs.class, MiniBatchSize.class, TestDataPath.class
    );

    final CommandLine cl = new CommandLine();
    cl.registerShortNameOfClass(HttpAddress.class);
    cl.registerShortNameOfClass(HttpPort.class);
    serverParamList.forEach(cl::registerShortNameOfClass);
    workerParamList.forEach(cl::registerShortNameOfClass);
    cl.registerShortNameOfClass(InputDir.class); // handle inputPath separately to process it through processInputDir()
    userParamList.forEach(cl::registerShortNameOfClass);

    final Configuration commandLineConf = cl.processCommandLine(args).getBuilder().build();

    // master side parameters are already registered. So it can be extracted
    // from commandLineConf unless it wasn't registered.
    final Configuration masterConf = extractParameterConf(masterParamList, commandLineConf);
    final Configuration serverConf = extractParameterConf(serverParamList, commandLineConf);
    final Configuration workerConf = extractParameterConf(workerParamList, commandLineConf);
    final Configuration userConf = extractParameterConf(userParamList, commandLineConf);

    // handle special parameters that need to be processed from commandline parameters
    final Injector commandlineParamInjector = Tang.Factory.getTang().newInjector(commandLineConf);
    final Configuration inputPathConf;
    final String inputPath = commandlineParamInjector.getNamedInstance(InputDir.class);
    inputPathConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(InputDir.class, inputPath)
        .build();

    // http configuration, target of http request is specified by this configuration.
    final Configuration httpConf = Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(HttpAddress.class, commandlineParamInjector.getNamedInstance(HttpAddress.class))
        .bindNamedParameter(HttpPort.class, commandlineParamInjector.getNamedInstance(HttpPort.class))
        .build();

    return Arrays.asList(masterConf, serverConf,
        Configurations.merge(workerConf, inputPathConf), userConf, httpConf);
  }

  /**
   * Extracts configuration which is only related to {@code parameterClassList} from {@code totalConf}.
   */
  private static Configuration extractParameterConf(final List<Class<? extends Name<?>>> parameterClassList,
                                                    final Configuration totalConf) {
    final ClassHierarchy totalConfClassHierarchy = totalConf.getClassHierarchy();
    final JavaConfigurationBuilder parameterConfBuilder = Tang.Factory.getTang().newConfigurationBuilder();
    for (final Class<? extends Name<?>> parameterClass : parameterClassList) {
      final NamedParameterNode parameterNode
          = (NamedParameterNode) totalConfClassHierarchy.getNode(parameterClass.getName());
      final String parameterValue = totalConf.getNamedParameter(parameterNode);
      // if this parameter is not included in the total configuration, parameterValue will be null
      if (parameterValue != null) {
        parameterConfBuilder.bindNamedParameter(parameterClass, parameterValue);
      }
    }
    return parameterConfBuilder.build();
  }

  /**
   * @return a configuration for spawning a {@link DolphinMaster}.
   */
  private static Configuration getJobConfiguration(final String id,
                                                   final Configuration masterConf,
                                                   final Configuration serverConf,
                                                   final Configuration workerConf,
                                                   final Configuration userParamConf) {
    final ConfigurationSerializer confSerializer = new AvroConfigurationSerializer();
    return Configurations.merge(masterConf, Tang.Factory.getTang().newConfigurationBuilder()
        .bindNamedParameter(AppIdentifier.class, id)
        .bindImplementation(OptimizationOrchestrator.class, DummyOrchestrator.class)
        .bindNamedParameter(ETDolphinLauncher.SerializedServerConf.class, confSerializer.toString(serverConf))
        .bindNamedParameter(ETDolphinLauncher.SerializedWorkerConf.class, confSerializer.toString(workerConf))
        .bindNamedParameter(ETDolphinLauncher.SerializedParamConf.class, confSerializer.toString(userParamConf))
        .build());
  }

  /**
   * Sends HTTP requests to specified URL using {@link HttpClient}.
   * @param command command of HTTP request
   * @param address an address of HTTP request
   * @param port a port number of HTTP request
   * @param serializedConf a job configuration for submitting a job.
   *                       It is serialized to send via HTTP POST body parameters.
   */
  private static void sendRequest(final String command, final String address,
                                  final String port, @Nullable final String serializedConf) {
    try {
      final HttpClient httpClient = HttpClientBuilder.create().build();
      final String url = "http://" + address + ":" + port + "/dolphin/v1/" + command;
      final HttpResponse response;
      switch (command) {
      case "submit":
        final NameValuePair confPair = new BasicNameValuePair("conf", serializedConf);
        final List<NameValuePair> nameValuePairs = Collections.singletonList(confPair);
        final HttpPost submitRequest = new HttpPost(url);
        submitRequest.setEntity(new UrlEncodedFormEntity(nameValuePairs));
        submitRequest.addHeader("content-type", "application/x-www-form-urlencoded");
        response = httpClient.execute(submitRequest);
        System.out.println("\nSending 'POST' request to URL : " + url);
        break;
      case "finish":
        final HttpGet finishRequest = new HttpGet(url);
        response = httpClient.execute(finishRequest);
        break;
      default:
        throw new RuntimeException("There is an unexpected command.");
      }

      System.out.println("Response Code : " + response.getStatusLine().getStatusCode() +
          ", Response Message : " + response.getStatusLine().getReasonPhrase());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @NamedParameter(doc = "A port number of HTTP request.", short_name = "port")
  private final class HttpPort implements Name<String> {

  }

  @NamedParameter(doc = "An address of HTTP request", short_name = "address")
  private final class HttpAddress implements Name<String> {

  }

  @NamedParameter(doc = "An identifier of App.")
  final class AppIdentifier implements Name<String> {

  }

}
