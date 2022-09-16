package by.akozel.springstreaming.google_cloud.consumer.exceptions;

public class BugGeneratorTrapException extends RuntimeException {

  public BugGeneratorTrapException(String filename) {
    super("[" + filename + "] It is Bug generator trap!", null, false, false);
  }

}
