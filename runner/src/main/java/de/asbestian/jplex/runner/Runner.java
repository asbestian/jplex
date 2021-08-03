package de.asbestian.jplex.runner;

import de.asbestian.jplex.input.LpFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** @author Sebastian Schenker */
public class Runner {

  private static final Logger LOGGER = LoggerFactory.getLogger(Runner.class);

  public static void main(final String... args) {
    final var reader = new LpFileReader(args[0]);
    LOGGER.info("Number of variables: {}", reader.getNumberOfVariables());
    LOGGER.info("Number of constraints: {}", reader.getNumberOfConstraints());
  }
}
