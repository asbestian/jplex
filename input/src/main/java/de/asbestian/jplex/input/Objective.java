package de.asbestian.jplex.input;

import java.util.Objects;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

/**
 * @author Sebastian Schenker
 */
public record Objective(String name, ObjectiveSense sense, ImmutableMap<String, Double> coefficients) {

  public enum ObjectiveSense {
    MAX(Lists.immutable.of("max", "maximise", "maximize", "maximum")),
    MIN(Lists.immutable.of("min", "minimise", "minimize", "minimum")),
    UNDEF(Lists.immutable.empty());

    Stream<String> rep() {
      return this.representation.stream();
    }

    ObjectiveSense(final ImmutableList<String> values) {
      this.representation = values;
    }

    private final ImmutableList<String> representation;
  }

  public Objective {
    Objects.requireNonNull(name);
    Objects.requireNonNull(sense);
    if (name.isBlank()) {
      throw new InputException("Expected non-blank objective name.");
    }
    // no check on coefficient map in order to allow zero objective
  }

  public static final class ObjectiveBuilder {
    private String name = null;
    private ObjectiveSense sense = null;
    private final MutableMap<String, Double> coefficients = new UnifiedMap<>();

    public ObjectiveBuilder setName(final String value) {
      this.name = value;
      return this;
    }

    public ObjectiveBuilder setSense(final ObjectiveSense value) {
      this.sense = value;
      return this;
    }

    public ObjectiveBuilder mergeCoefficients(final ImmutableMap<String, Double> map) {
      map.forEachKeyValue((key, value) -> this.coefficients.merge(key, value, Double::sum));
      return this;
    }

    public Objective build() {
      return new Objective(this.name, this.sense, this.coefficients.toImmutable());
    }

  }

}
