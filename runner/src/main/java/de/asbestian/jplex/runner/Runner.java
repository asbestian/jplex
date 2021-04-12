package de.asbestian.jplex.runner;

import de.asbestian.jplex.input.LpFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Sebastian Schenker */
public class Runner {

  private static final Logger LOGGER = LoggerFactory.getLogger(Runner.class);

  public static void main(String... args) {
    final LpFileReader lpFileReader = new LpFileReader(args[0]);
    try {
      lpFileReader.readLP();
    } catch (Exception e) {
      LOGGER.warn(e.getMessage());
    }
    LOGGER.info(String.format("Number of variables: %d", lpFileReader.noOfVariables()));
    LOGGER.info(String.format("Number of constraints: %d", lpFileReader.noOfConstraints()));
  }
}
