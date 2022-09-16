package by.akozel.springstreaming.google_cloud.provider.exceptions;

public class ConsumerInternalErrorException extends RuntimeException {

  public ConsumerInternalErrorException(String filename) {
    super("[" + filename + "] Consumer didn't managed to get a file", null, true, false);
  }
}
