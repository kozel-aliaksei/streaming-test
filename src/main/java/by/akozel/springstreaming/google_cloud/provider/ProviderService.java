package by.akozel.springstreaming.google_cloud.provider;

import by.akozel.springstreaming.google_cloud.provider.exceptions.ConsumerInternalErrorException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ConcurrentModificationException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

@Service
public class ProviderService {

  private final WebClient webClient;
  private final Storage storage;
  private final String bucketName = "launch-web-client-testing";

  private final Logger logger = LoggerFactory.getLogger(ProviderService.class);

  public ProviderService() throws IOException {
    logger.info("Service init");

    Credentials credentials = GoogleCredentials.fromStream(
        Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("google-auth.json")));

    this.storage = StorageOptions.newBuilder()
        .setCredentials(credentials)
        .build()
        .getService();

    webClient  = WebClient.create("http://localhost:8080");
  }


  public ReadChannel geFileChannelByName(String name) {
   return storage.reader(BlobId.of(bucketName, name));
  }

  public Mono<HttpStatus> streamFileToClient(String blobName, String filename) {
    //BodyInserter<Flux<DataBuffer>, ReactiveHttpOutputMessage> fluxReactiveHttpOutputMessageBodyInserter

    ByteBufAllocator allocator = new PooledByteBufAllocator(true);
    DataBufferFactory bufferFactory = new NettyDataBufferFactory(allocator);
    DataBufferFactory sharedInstance = DefaultDataBufferFactory.sharedInstance;

    return Mono.just(blobName)
            .doOnNext(blob -> logger.info("[{}] Opening Chanel to stream file", filename))
            .map(this::geFileChannelByName)
            .map(channel -> DataBufferUtils
                .readByteChannel(() -> channel, bufferFactory, 64 * 1024)
                .limitRate(1,1)
            )
            .map(BodyInserters::fromDataBuffers)
            .doOnNext(fluxReactiveHttpOutputMessageBodyInserter -> logger.info("[{}] Streaming file to consumer", filename))
            .flatMap(fluxReactiveHttpOutputMessageBodyInserter ->
                    webClient
                        .post()
                        .uri("/" + filename)
                        .body(fluxReactiveHttpOutputMessageBodyInserter)
                        .exchangeToMono(clientResponse -> {
                          if(clientResponse.statusCode().is5xxServerError()) {
                            throw new ConsumerInternalErrorException(filename);
                          }
                          return Mono.just(clientResponse.statusCode());
                        })
            )
            .doOnError(ConsumerInternalErrorException.class, e -> logger.warn(e.getMessage()))
            .retry(3);
  }




}
