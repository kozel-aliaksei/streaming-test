package by.akozel.springstreaming.google_cloud;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/")
public class Resource {

  private final String dirName = "temp";
  private final AtomicInteger counter = new AtomicInteger(0);
  private final long validFileSize = 209715200L;

  private final Logger logger = LoggerFactory.getLogger(Resource.class);

  private final Storage storage;

  public Resource() throws IOException {
    Credentials credentials = GoogleCredentials.fromStream(new FileInputStream("google-cloud-auth.json"));

    this.storage = StorageOptions.newBuilder()
        .setCredentials(credentials)
        .build()
        .getService();
  }


  @PostMapping(value = "/", consumes = MediaType.APPLICATION_OCTET_STREAM_VALUE)
  @ResponseStatus(value = HttpStatus.OK)
  public Mono<String> upload(@RequestBody Flux<DataBuffer> filePartFlux) {
    return Mono
        .just(getFilePath())
        .flatMap(filePath -> writeByPath(filePartFlux, filePath))
        .doOnNext(filePath -> checkFileSize(filePath, validFileSize))
        .doOnNext(this::makeFileEmpty)
        .then(Mono.just("ok"));
  }

  private Path getFilePath() {
    return Paths
        .get(dirName + "/" + counter.incrementAndGet() +
            " - " +
            Instant.now()
                .toString()
                .replace(":",".")
        );
  }

  private Mono<Path> writeByPath(Flux<DataBuffer> filePartFlux, Path filePath) {
    return DataBufferUtils
        .write(filePartFlux, filePath, WRITE, CREATE)
        .thenReturn(filePath);
  }

  private Path checkFileSize(Path filePath, long expected) {
    long fileSize = getFileSize(filePath);
    if (expected != fileSize) {
      logger.warn("ERROR! Expected file size is: {}; real: {}; filename: {}",
          expected, fileSize, filePath.getFileName().toString());
    }
    return filePath;
  }

  private long getFileSize(Path filePath) {
    try {
      return Files.size(filePath);
    } catch (IOException e) {
      throw new RuntimeException("Can not get file size: " + filePath.getFileName().toString(), e);
    }
  }

  private Path makeFileEmpty(Path path) {
    try {
      return Files.write(path, Instant.now().toString().getBytes(), WRITE, TRUNCATE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException("Error while making file empty, fileName: " + path.getFileName().toString(), e);
    }
  }

}
