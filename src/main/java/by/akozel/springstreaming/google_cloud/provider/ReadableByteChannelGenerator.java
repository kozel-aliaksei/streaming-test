package by.akozel.springstreaming.google_cloud.provider;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.function.Consumer;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import reactor.core.publisher.SynchronousSink;

public class ReadableByteChannelGenerator  implements Consumer<SynchronousSink<DataBuffer>> {


  private final ReadableByteChannel channel;
  private final int bufferSize;
  private final DataBuffer dataBuffer;

  public ReadableByteChannelGenerator(ReadableByteChannel channel, DataBufferFactory dataBufferFactory, int bufferSize) {

    this.channel = channel;
    this.bufferSize = bufferSize;
    this.dataBuffer = dataBufferFactory.allocateBuffer(this.bufferSize);
  }

  @Override
  public void accept(SynchronousSink<DataBuffer> sink) {

    if(dataBuffer.asByteBuffer().remaining() == 0) {
      int read;
      ByteBuffer byteBuffer = dataBuffer.asByteBuffer(0, dataBuffer.capacity());
      try {
        if ((read = this.channel.read(byteBuffer)) >= 0) {
          dataBuffer.writePosition(read);
          sink.next(dataBuffer);
        } else {
          sink.complete();
        }
      } catch (IOException ex) {
        sink.error(ex);
      }
    }
  }

  @Override
  public Consumer<SynchronousSink<DataBuffer>> andThen(Consumer<? super SynchronousSink<DataBuffer>> after) {
    return Consumer.super.andThen(after);
  }
}
