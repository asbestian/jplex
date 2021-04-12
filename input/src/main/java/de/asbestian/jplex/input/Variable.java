package de.asbestian.jplex.input;

import java.util.Objects;

/**
 * @author Sebastian Schenker
 */
public record Variable(String name, double lb, double ub) {

  public Variable {
    if (name.isBlank()) {
      throw new InputException("Expected non-blank variable name.");
    }
  }

  public static final class VariableBuilder {

    private String name = null;
    private Double lb = 0.;
    private Double ub = Double.POSITIVE_INFINITY;

    public VariableBuilder setName(final String value) {
      name = value;
      return this;
    }

    public VariableBuilder setLb(final double value) {
      lb = value;
      return this;
    }

    public VariableBuilder setUb(final double value) {
      ub = value;
      return this;
    }

    public Variable build() {
      Objects.requireNonNull(name);
      return new Variable(name, lb, ub);
    }

  }

}
