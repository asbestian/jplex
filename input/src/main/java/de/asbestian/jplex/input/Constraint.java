package de.asbestian.jplex.input;

import java.util.Arrays;
import java.util.Objects;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

/**
 * @author Sebastian Schenker
 */
public record Constraint(String name, int lineNumber, ImmutableMap<String, Double> coefficients, ConstraintSense sense, double rhs) {

  public enum ConstraintSense {
    LE("<="),
    EQ("="),
    GE(">=");

    public static ConstraintSense from(final String rep) {
      return Arrays.stream(values())
          .filter(sense -> sense.representation.equals(rep)).findFirst().orElseThrow();
    }

    ConstraintSense(final String representation) {
      this.representation = representation;
    }

    final String representation;
  }

  public Constraint {
    Objects.requireNonNull(name);
    if (lineNumber <= 0) {
      throw new InputException("Expected positive line number.");
    }
    Objects.requireNonNull(sense);
    if (coefficients.isEmpty()) {
      throw new InputException("Expected non-empty coefficient map.");
    }
  }

  public static final class ConstraintBuilder {

    private String name = null;
    private final MutableMap<String, Double> coefficients = new UnifiedMap<>();
    private Integer lineNumber = null;
    private ConstraintSense sense = null;
    private Double rhs = null;

    public ConstraintBuilder setName(final String value) {
      name = value;
      return this;
    }

    public ConstraintBuilder setLineNumber(final int value) {
      lineNumber = value;
      return this;
    }

    public ConstraintBuilder mergeCoefficients(final ImmutableMap<String, Double> map) {
      map.forEachKeyValue((key, value) -> coefficients.merge(key, value, Double::sum));
      return this;
    }

    public ConstraintBuilder setSense(final ConstraintSense value) {
      sense = value;
      return this;
    }

    public ConstraintBuilder setRhs(final double value) {
      rhs = value;
      return this;
    }

    public Constraint build() {
      return new Constraint(name, lineNumber, coefficients.toImmutable(), sense, rhs);
    }
  }

}
