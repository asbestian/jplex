package de.asbestian.jplex.input;

import java.util.Objects;
import org.eclipse.collections.api.map.ImmutableMap;

/**
 * @author Sebastian Schenker
 */
public record Objective(String name, OBJECTIVE_SENSE sense, ImmutableMap<String, Double> coefficients) {

  public enum OBJECTIVE_SENSE {
    MAX,
    MIN,
    UNDEF
  }

  public Objective {
    Objects.requireNonNull(name);
    Objects.requireNonNull(sense);
    // no check on coefficient map in order to allow zero objective
  }

}
