package edu.snu.reef.elastic.memory;

import org.apache.reef.evaluator.context.ContextMessageHandler;
import org.apache.reef.io.serialization.Codec;

import javax.inject.Inject;

public final class ElasticMemoryControlMessageHandler implements ContextMessageHandler {

  private final Codec<ElasticMemoryControlMessage> codec;

  @Inject
  public ElasticMemoryControlMessageHandler(final ElasticMemoryControlMessageCodec codec) {
    this.codec = codec;
  }

  @Override
  public void onNext(final byte[] msg) {
    System.out.println(ElasticMemoryControlMessageHandler.class.getSimpleName());
    System.out.println(codec.decode(msg));
    System.out.println();
  }
}
