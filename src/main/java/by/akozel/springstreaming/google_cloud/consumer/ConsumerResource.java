package by.akozel.springstreaming.google_cloud.consumer;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import by.akozel.springstreaming.google_cloud.consumer.exceptions.BugGeneratorTrapException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/")
public class ConsumerResource {
  private final long validFileSizeLarge = 104857600L;
  private final long validFileSize1gb = 1000000000L;

  private final long validFileSize = validFileSize1gb;

  private final boolean unstable = false;

  private final Random random = new Random();

  private final Set<String> failedRequests = new HashSet<>();

  private final Logger logger = LoggerFactory.getLogger(ConsumerResource.class);

  public ConsumerResource() {
    logger.info("Resource init");
  }


  @PostMapping(value = "/{filename}", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
  @ResponseStatus(value = HttpStatus.OK)
  public Mono<String> upload(@PathVariable String filename,
                             @RequestBody Flux<DataBuffer> filePartFlux) {

    Flux<DataBuffer> finalFilePartFlux = filePartFlux
        .map(dataBuffer -> generateBugIfRequired(dataBuffer, filename));

    return Mono
        .just(getFilePath(filename))
        .doOnNext(path -> logger.info("[{}] Consuming POST request to save file", filename))
        .flatMap(filePath -> writeByPath(finalFilePartFlux, filePath))
        .doOnNext(filePath -> logIfUnexpectedFileSize(filePath, validFileSize))
        .doOnNext(this::purgeFile)

//        .flatMap(path -> finalFilePartFlux
//            .doOnNext(dataBuffer -> dataBuffer.asByteBuffer().clear())
//            .then(Mono.just(path))
//        )
        .doOnNext(path -> logger.info("[{}] File is done. Send HTTP response: ok", filename))
        .then(Mono.just("ok"))
        .doOnError(BugGeneratorTrapException.class, e -> logger.warn(e.getMessage()));

//    return Mono.just("ok");
  }

  private DataBuffer generateBugIfRequired(DataBuffer dataBuffer, String filename) {
    if(unstable) {
      long randomValue = random.nextLong(validFileSize);
      if (randomValue < validFileSize * 0.00003 && failedRequests.add(filename)) {
        throw new BugGeneratorTrapException(filename);
      }
    }
    return dataBuffer;
  }

  private Path getFilePath(String filename) {
    return Paths.get("temp/" + filename);
  }

  private Mono<Path> writeByPath(Flux<DataBuffer> filePartFlux, Path filePath) {
    return DataBufferUtils
        .write(filePartFlux, filePath, WRITE, CREATE, TRUNCATE_EXISTING)
        .thenReturn(filePath);
  }

  private void logIfUnexpectedFileSize(Path filePath, long expected) {
    long fileSize = getFileSize(filePath);
    if (expected != fileSize) {
      logger.warn("ERROR! Expected file size is: {}; real: {}; filename: {}",
          expected, fileSize, filePath.getFileName().toString());
    }
  }

  private long getFileSize(Path filePath) {
    try {
      return Files.size(filePath);
    } catch (IOException e) {
      throw new RuntimeException("Can not get file size: " + filePath.getFileName().toString(), e);
    }
  }

  private Path purgeFile(Path path) {
    try {
      return Files.write(path, Instant.now().toString().getBytes(), WRITE, TRUNCATE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException("Error while making file empty, fileName: " + path.getFileName().toString(), e);
    }
  }

}
