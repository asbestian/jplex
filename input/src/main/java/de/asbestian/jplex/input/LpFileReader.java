package de.asbestian.jplex.input;

import de.asbestian.jplex.input.Constraint.ConstraintBuilder;
import de.asbestian.jplex.input.Constraint.ConstraintSense;
import de.asbestian.jplex.input.Objective.ObjectiveBuilder;
import de.asbestian.jplex.input.Objective.ObjectiveSense;
import de.asbestian.jplex.input.Variable.VariableBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for reading input files given in lp format.
 * Note that the parser cannot yet handle integer and binary variables.
 *
 * @author Sebastian Schenker */
public class LpFileReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(LpFileReader.class);

  private enum Section {
    START(Lists.immutable.empty()),
    OBJECTIVE(Lists.immutable.empty()),
    CONSTRAINTS(Lists.immutable.of("subject to", "such that", "s.t.", "st.", "st")),
    BOUNDS(Lists.immutable.of("bounds", "bound")),
    BINARY(Lists.immutable.of("binary", "binaries", "bin")),
    GENERAL(Lists.immutable.of("generals", "general", "gen")),
    END(Lists.immutable.of("end"));

    Stream<String> rep() {
      return this.representation.stream();
    }

    Section(final ImmutableList<String> values) {
      this.representation = values;
    }

    private final ImmutableList<String> representation;

  }

  private enum Sign {
    MINUS(-1),
    PLUS(1),
    UNDEF(0);

    Sign(final int value) {
      this.value = value;
    }

    final int value;
  }

  private sealed interface ParsedConstraintLine permits Unit, Pair, Triple {}
  private static record Unit(String line) implements ParsedConstraintLine {}
  private static record Pair(ConstraintSense sense, Double rhs) implements ParsedConstraintLine {}
  private static record Triple(String line, ConstraintSense sense, Double rhs) implements
      ParsedConstraintLine {}

  private static final String PATTERN =
      "[a-zA-Z\\!\"#\\$%&\\(\\)/,;\\?`'\\{\\}\\|~_][a-zA-Z0-9\\!\"#\\$%&\\(\\)/,\\.;\\?`'\\{\\}\\|~_]*";

  private ImmutableList<Objective> objectives;
  private ImmutableList<Constraint> constraints;
  private ImmutableMap<String, Variable> variables;
  private Section currentSection;
  private int currentLineNumber;

  public LpFileReader(final String path) {
    this.currentSection = Section.START;
    this.currentLineNumber = 0;
    final MutableMap<String, VariableBuilder> variableBuilders = new UnifiedMap<>();
    try (final BufferedReader bf = new BufferedReader(new FileReader(path))) {
      final var objectiveSense = readObjectiveSense(bf);
      this.objectives = readObjectives(bf, variableBuilders, objectiveSense);
      this.constraints = readConstraints(bf, variableBuilders);
      if (this.currentSection == Section.BOUNDS) {
        readBounds(bf, variableBuilders);
      }
      this.variables = variableBuilders.collectValues((key, value) -> value.build()).toImmutable();
    } catch (final IOException | InputException e) {
      LOGGER.error("Problem reading section {} in input file {}", this.currentSection, path);
      LOGGER.error(e.getMessage());
      this.objectives = Lists.immutable.empty();
      this.constraints = Lists.immutable.empty();
      this.variables = Maps.immutable.empty();
    }
  }

  public int getNumberOfVariables() {
    return this.variables.size();
  }

  public int getNumberOfConstraints() {
    return this.constraints.size();
  }

  private ObjectiveSense readObjectiveSense(final BufferedReader bufferedReader) throws IOException, InputException {
    if (this.currentSection != Section.START) {
      throw new InputException(String.format("Expected Section.START, found %s.",
          this.currentSection));
    }
    final var line = getNextProperLine(bufferedReader);
    final var isMax = ObjectiveSense.MAX.rep().anyMatch(line::equalsIgnoreCase);
    final var isMin = ObjectiveSense.MIN.rep().anyMatch(line::equalsIgnoreCase);
    if (isMax || isMin) {
      this.currentSection = Section.OBJECTIVE;
      LOGGER.debug("Switching to section {}.", this.currentSection);
      if (isMax) {
        LOGGER.debug("Found maximisation direction.");
        return ObjectiveSense.MAX;
      } else {
        LOGGER.debug("Found minimisation direction.");
        return ObjectiveSense.MIN;
      }
    }
    else {
      throw new InputException(
          String.format("Line %d: unrecognised optimisation direction %s.", this.currentLineNumber, line));
    }
  }

  private ImmutableList<Objective> readObjectives(
      final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders,
      final ObjectiveSense objectiveSense)
      throws IOException, InputException {
    if (this.currentSection != Section.OBJECTIVE) {
      throw new InputException(String.format("Expected Section.OBJECTIVE, found %s.",
          this.currentSection));
    }
    final MutableList<ObjectiveBuilder> builders = Lists.mutable.empty();
    final Predicate<String> isConstraintSection = line -> Section.CONSTRAINTS.rep().anyMatch(line::equalsIgnoreCase);
    String line = getNextProperLine(bufferedReader);
    while(!isConstraintSection.test(line)) {
      final int colonIndex = line.indexOf(':');
      if (colonIndex != -1) { // objective function name found; add new builder
        final var name = getName(line, 0, colonIndex, this.currentLineNumber);
        LOGGER.trace("Found objective name: {}.", name);
        final var builder = new ObjectiveBuilder().setSense(objectiveSense).setName(name);
        builders.add(builder);
        line = line.substring(colonIndex + 1).trim();
      }
      // parse objective expression
      LOGGER.trace("Parsing line {}: {}", this.currentLineNumber, line);
      final var linComb = parseLinComb(variableBuilders, line, this.currentLineNumber);
      builders.getLast().mergeCoefficients(linComb);
      line = getNextProperLine(bufferedReader);
    }
    this.currentSection = Section.CONSTRAINTS;
    LOGGER.debug("Switching to section {}.", this.currentSection);
    return builders.collect(ObjectiveBuilder::build).toImmutable();
  }

  ImmutableList<Constraint> readConstraints(
      final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders)
      throws IOException, InputException {
    if (this.currentSection != Section.CONSTRAINTS) {
      throw new InputException(String.format("Expected Section.CONSTRAINTS, found %s.",
          this.currentSection));
    }
    final MutableList<Constraint> constraints = Lists.mutable.empty();
    ConstraintBuilder consBuilder = null;
    while (true) {
      var line = getTrimmedLine(bufferedReader);
      final int colonIndex = line.indexOf(':');
      if (Section.END.rep().anyMatch(line::equalsIgnoreCase)) {
        this.currentSection = Section.END;
        LOGGER.debug("Switching to section {}.", this.currentSection);
        break;
      } else if (Section.BOUNDS.rep().anyMatch(line::equalsIgnoreCase)) {
        this.currentSection = Section.BOUNDS;
        LOGGER.debug("Switching to section {}.", this.currentSection);
        break;
      } else if (Section.GENERAL.rep().anyMatch(line::equalsIgnoreCase)) {
        this.currentSection = Section.GENERAL;
        LOGGER.debug("Switching to section {}.", this.currentSection);
        break;
      } else if (Section.BINARY.rep().anyMatch(line::equalsIgnoreCase)) {
        this.currentSection = Section.BINARY;
        LOGGER.debug("Switching to section {}.", this.currentSection);
        break;
      }
      else if (colonIndex != -1) { // constraint name found
          final var name = getName(line, 0, colonIndex, this.currentLineNumber);
          LOGGER.trace("Found constraint name: {}.", name);
          consBuilder = new ConstraintBuilder().setName(name).setLineNumber(this.currentLineNumber);
          line = line.substring(colonIndex + 1).trim();
      }
      LOGGER.trace("Parsing line {}: {}", this.currentLineNumber, line);
      final var result = parseConstraintLine(line, variableBuilders);
      if (consBuilder == null) {
        throw new InputException("Constraint builder is null.");
      }
      switch (result) {
        case Unit unit -> {
          final var lhs = parseLinComb(variableBuilders, unit.line(), this.currentLineNumber);
          consBuilder.mergeCoefficients(lhs);
        }
        case Pair pair -> {
          consBuilder.setSense(pair.sense);
          consBuilder.setRhs(pair.rhs);
          constraints.add(consBuilder.build());
          consBuilder = null;
        }
        case Triple triple -> {
          final var lhs = parseLinComb(variableBuilders, triple.line(), this.currentLineNumber);
          consBuilder.mergeCoefficients(lhs);
          consBuilder.setSense(triple.sense);
          consBuilder.setRhs(triple.rhs);
          constraints.add(consBuilder.build());
          consBuilder = null;
        }
      }
    }
   return constraints.toImmutable();
}

  private void readBounds(final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders) throws IOException, InputException {
    if (this.currentSection != Section.BOUNDS) {
      throw new InputException(String.format("Expected Section.BOUNDS, found %s.",
          this.currentSection));
    }
    while (true) {
      final var line = getTrimmedLine(bufferedReader);
      if (line.isBlank()) {
        continue;
      }
      else if (line.equalsIgnoreCase("end")) {
        LOGGER.debug("Switching to section {}.", Section.END);
        this.currentSection = Section.END;
        break;
      }
      LOGGER.trace("Parsing line {}: {}", this.currentLineNumber, line);
      parseBound(line, variableBuilders);
    }
  }

  private static double parseValue(final String expr, final int currentLine) {
    if (expr.equalsIgnoreCase("inf") || expr.equalsIgnoreCase("infinity")
        || expr.equalsIgnoreCase("+inf") || expr.equalsIgnoreCase("+infinity")) {
      return Double.POSITIVE_INFINITY;
    }
    else if (expr.equalsIgnoreCase("-inf") || expr.equalsIgnoreCase("-infinity")) {
      return Double.NEGATIVE_INFINITY;
    }
    else {
      final double bound;
      try {
        bound = Double.parseDouble(expr);
      }
      catch (final NumberFormatException e) {
        throw new InputException(String.format("Line %d: %s is not a valid number.", currentLine, expr));
      }
      return bound;
    }
  }

  // throws if not found
  private static VariableBuilder getVariableBuilder(final String name, final MutableMap<String, VariableBuilder> variableBuilders, final int currentLine) {
    final var variable = variableBuilders.get(name);
    if (variable == null) {
      throw new InputException(String.format("Line %d: unknown variable name %s", currentLine, name));
    }
    return variable;
  }

  private void parseBound(final String line, final MutableMap<String, VariableBuilder> variableBuilders) {
    var tokens = line.split("<=");
    switch (tokens.length) {
      case 1 -> { // var = bound || var free
        tokens = line.split("=");
        if (tokens.length > 1) {
          LOGGER.trace("Parsing equality bound.");
          final var variable = getVariableBuilder(tokens[0].trim(), variableBuilders,
              this.currentLineNumber);
          final var bound = parseValue(tokens[1].trim(), this.currentLineNumber);
          variable.setLb(bound);
          variable.setUb(bound);
        } else {
          tokens = line.split("\s"); // split by whitespace character
          if (tokens.length != 2 || !tokens[1].equalsIgnoreCase("free")) {
            throw new InputException(String.format("Line %d: expected free variable expression, found %s.",
                this.currentLineNumber, line));
          }
          final var variable = getVariableBuilder(tokens[0].trim(), variableBuilders,
              this.currentLineNumber);
          LOGGER.trace("Parsing free variable {}.", tokens[0].trim());
          variable.setLb(Double.NEGATIVE_INFINITY);
          variable.setUb(Double.POSITIVE_INFINITY);
        }
      }
      case 2 -> { // var <= bound || bound <= var
        var variable = variableBuilders.get(tokens[0].trim());
        if (variable == null) {
          LOGGER.trace("Parsing one-sided lower bound.");
          variable = getVariableBuilder(tokens[1].trim(), variableBuilders, this.currentLineNumber);
          final var bound = parseValue(tokens[0].trim(), this.currentLineNumber);
          variable.setLb(bound);
        }
        else {
          LOGGER.trace("Parsing one-sided upper bound.");
          final var bound = parseValue(tokens[1].trim(), this.currentLineNumber);
          variable.setUb(bound);
        }
      }
      case 3 -> { // lb <= var <= ub
        final var variable = getVariableBuilder(tokens[1].trim(), variableBuilders,
            this.currentLineNumber);
        final var lb = parseValue(tokens[0].trim(), this.currentLineNumber);
        final var ub = parseValue(tokens[2].trim(), this.currentLineNumber);
        variable.setLb(lb);
        variable.setUb(ub);
      }
      default ->
        throw new InputException(String.format("Line %d: unknown bound format %s", this.currentLineNumber, line));
    }
  }

   private ParsedConstraintLine parseConstraintLine(final String line, final MutableMap<String, VariableBuilder> variableBuilders) {
    final var sensePattern = Pattern.compile("[><=]{1,2}");
    final var senseMatcher = sensePattern.matcher(line);
    if (senseMatcher.find()) { // lhs sense rhs || sense rhs
      final var constraintSense = ConstraintSense.from(senseMatcher.group());
      LOGGER.trace("Found constraint sense: {}", constraintSense.representation);
      final var tokens = Arrays.stream(line.split(senseMatcher.group()))
          .filter(s -> !s.isBlank()).toList();
      if (tokens.size() == 1) { // sense rhs
        final var rhs = parseValue(tokens.get(0), this.currentLineNumber);
        return new Pair(constraintSense, rhs);
      }
      else if (tokens.size() == 2) { // lhs sense rhs
        final var rhs = parseValue(tokens.get(1), this.currentLineNumber);
        return new Triple(tokens.get(0), constraintSense, rhs);
      }
      else {
        throw new InputException(String.format("Line %d: invalid constraint line %s.",
            this.currentLineNumber, line));
      }
    }
    else { // only lhs
      return new Unit(line.trim());
    }
  }

  private static String getName(final String str, final int beginIndex, final int endIndex, final int currentLine) {
    final String name = str.substring(beginIndex, endIndex).trim();
    if (!name.matches(PATTERN)) {
      throw new InputException(String.format("Line %d: invalid name %s.", currentLine, name));
    }
    return name;
  }

  /**
   * Returns the next non-blank line stripped of any white space and comment.
   */
  private String getNextProperLine(final BufferedReader reader) throws IOException {
    String line;
    do {
      if (!reader.ready()) {
        throw new InputException("Buffered reader is not ready.");
      }
      line = reader.readLine();
      ++this.currentLineNumber;
      final int commentBegin = line.indexOf('\\');
      if (commentBegin != -1) {
        line = line.substring(0, commentBegin);
      }
    } while (line.isBlank());
    return line.strip();
  }

  /**
   * Returns the next line without leading space, trailing space, and trailing comment and increases
   * line counter
   */
  private String getTrimmedLine(final BufferedReader reader) throws IOException {
    if (!reader.ready()) {
      throw new InputException("Buffered reader is not ready.");
    }
    String line = reader.readLine();
    ++this.currentLineNumber;
    final int commentBegin = line.indexOf('\\');
    if (commentBegin != -1) {
      line = line.substring(0, commentBegin);
    }
    return line.trim();
  }

  private static ImmutableMap<String, Double> parseLinComb(
      final MutableMap<String, VariableBuilder> variableBuilders,
      final String expr,
      final int lineNo)
      throws InputException {
    final MutableMap<String, Double> linComb = new UnifiedMap<>();
    final var exprTokens = new StringTokenizer(expr, "+-", true);
    var sign = Sign.PLUS;
    while (exprTokens.hasMoreTokens()) {
      final String token = exprTokens.nextToken().trim();
      if (token.isEmpty()) {
        continue;
      }
      if (token.equals("+")) {
        sign = Sign.PLUS;
      } else if (token.equals("-")) {
        sign = Sign.MINUS;
      } else {
        if (sign == Sign.UNDEF) {
          throw new InputException(String.format("line %d: missing sign", lineNo));
        }
        LOGGER.trace("Parsing {} {}", sign, token);
        parseAddend(variableBuilders, token, lineNo, sign, linComb);
        sign = Sign.UNDEF;
      }
    }
    return linComb.toImmutable();
  }

  private static void parseAddend(
      final MutableMap<String, VariableBuilder> variableBuilders,
      final String expr,
      final int currentLine,
      final Sign sign,
      final MutableMap<String, Double> linComb)
      throws InputException {
    final int splitIndex = getSummandSplitIndex(expr);
    if (splitIndex == -1) {
      throw new InputException(String.format("Line %d: %s is not a valid addend.", currentLine, expr));
    }
    double coeff = 1;
    if (splitIndex > 0) {
      final String coeffStr = expr.substring(0, splitIndex).trim();
      coeff = parseValue(coeffStr, currentLine);
    }
    final String name = getName(expr, splitIndex, expr.length(), currentLine);
    final var variable = variableBuilders.get(name);
    if (variable == null) {
      final var varBuilder = new VariableBuilder().setName(name);
      variableBuilders.put(name, varBuilder);
    }
    LOGGER.trace("Found {} {}", sign.value * coeff, name);
    linComb.merge(name, sign.value * coeff, Double::sum);
  }

  private static int getSummandSplitIndex(final String expr) {
    boolean inExp = false;
    for (int i = 0; i < expr.length(); ++i) {
      final char c = expr.charAt(i);
      if (!(Character.isDigit(c) || (c == '.'))) {
        if ((c == 'e') || (c == 'E')) {
          if (inExp) {
            return i;
          } else {
            inExp = true;
          }
        } else {
          return i;
        }
      }
    }
    return -1;
  }

}
