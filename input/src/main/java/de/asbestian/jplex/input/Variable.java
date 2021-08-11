package de.asbestian.jplex.input;

import java.util.Objects;

/**
 * @author Sebastian Schenker
 */
public record Variable(String name, VariableType type, double lb, double ub) {

  public enum VariableType {
    BINARY,
    INTEGER,
    CONTINUOUS
  }

  public Variable {
    if (name.isBlank()) {
      throw new InputException("Expected non-blank variable name.");
    }
    if (lb > ub) {
      throw new InputException(String.format("Lower bound = %f > %f = upper bound.", lb, ub));
    }
  }

  public static final class VariableBuilder {

    private String name = null;
    private VariableType type = VariableType.CONTINUOUS;
    private Double lb = 0.;
    private Double ub = Double.POSITIVE_INFINITY;

    public VariableBuilder setName(final String value) {
      this.name = value;
      return this;
    }

    public VariableBuilder setType(final VariableType value) {
      this.type = value;
      return this;
    }

    public VariableBuilder setLb(final double value) {
      this.lb = value;
      return this;
    }

    public VariableBuilder setUb(final double value) {
      this.ub = value;
      return this;
    }

    public Variable build() {
      Objects.requireNonNull(this.name);
      if (this.type == VariableType.BINARY) {
        if (this.lb < 0.) {
          this.lb = 0.;
        }
        if (this.ub > 1.) {
          this.ub = 1.;
        }
      }
      return new Variable(this.name, this.type, this.lb, this.ub);
    }

  }

}
