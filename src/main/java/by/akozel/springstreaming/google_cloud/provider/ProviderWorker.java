package by.akozel.springstreaming.google_cloud.provider;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ProviderWorker {

  private final Logger logger = LoggerFactory.getLogger(ProviderWorker.class);
  private final AtomicInteger counter = new AtomicInteger(0);

  private static final String LARGE_FILE_NAME = "LargeFile";
  private static final String GB1_FILE_NAME = "1gbFile";

  private final ProviderService providerService;


  public ProviderWorker(ProviderService providerService) {
    logger.info("Observer init");
    this.providerService = providerService;
  }

  @EventListener
  private void observe(ContextRefreshedEvent event) {
    logger.info("Observer started");
    Flux<HttpStatus> task = Mono
        .defer(() -> providerService.streamFileToClientMultipart(GB1_FILE_NAME, generateFileName()))
        .repeat();

    Flux.merge(
        IntStream.range(0, 3)
            .mapToObj(operand -> task)
            .toList()
        )
        .subscribe();
  }

  private String generateFileName() {
    return counter.incrementAndGet() +
        " - " +
        Instant.now()
            .toString()
            .replace(":", ".") +
        ".txt";
  }

}
