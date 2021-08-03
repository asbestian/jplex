package de.asbestian.jplex.input;

import de.asbestian.jplex.input.Constraint.ConstraintBuilder;
import de.asbestian.jplex.input.Constraint.ConstraintSense;
import de.asbestian.jplex.input.Objective.OBJECTIVE_SENSE;
import de.asbestian.jplex.input.Variable.VariableBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.ImmutableMap;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.list.mutable.ArrayListAdapter;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.immutable.ImmutableUnifiedMap;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.javatuples.Tuple;
import org.javatuples.Unit;
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
    START,
    OBJECTIVE,
    CONSTRAINTS,
    BOUNDS,
    END
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

  private static final String PATTERN =
      "[a-zA-Z\\!\"#\\$%&\\(\\)/,;\\?`'\\{\\}\\|~_][a-zA-Z0-9\\!\"#\\$%&\\(\\)/,\\.;\\?`'\\{\\}\\|~_]*";

  private Objective objective;
  private ImmutableList<Constraint> constraints;
  private ImmutableMap<String, Variable> variables;
  private Section currentSection;
  private int currentLine;

  public LpFileReader(final String path) {
    currentSection = Section.START;
    currentLine = 0;
    final MutableMap<String, VariableBuilder> variableBuilders = new UnifiedMap<>();
    try (final BufferedReader bf = new BufferedReader(new FileReader(path))) {
      objective = readObjective(bf, variableBuilders);
      constraints = readConstraints(bf, variableBuilders);
      if (currentSection == Section.BOUNDS) {
        readBounds(bf, variableBuilders);
      }
      variables = variableBuilders.collectValues((key, value) -> value.build()).toImmutable();
    } catch (final IOException | InputException e) {
      LOGGER.error("Problem reading section {} in input file {}", currentSection, path);
      LOGGER.error(e.getMessage());
      objective = new Objective("", OBJECTIVE_SENSE.UNDEF, new ImmutableUnifiedMap<>(
          Collections.emptyMap()));
      constraints = new FastList<Constraint>().toImmutable();
      variables = new ImmutableUnifiedMap<>(Collections.emptyMap());
    }
  }

  public Objective getObjective() {
    return objective;
  }

  public int getNumberOfVariables() {
    return variables.size();
  }

  public int getNumberOfConstraints() {
    return constraints.size();
  }

  private Objective readObjective(
      final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders)
      throws IOException, InputException {
    if (currentSection != Section.START) {
      throw new InputException(String.format("Expected Section.START, found %s.", currentSection));
    }
    OBJECTIVE_SENSE sense = null;
    // read optimisation direction
    var optDirectionRead = false;
    while (!optDirectionRead) {
      final var line = getTrimmedLine(bufferedReader);
      if (line.isBlank()) {
        continue;
      }
      LOGGER.debug("Switching to section {}.", Section.OBJECTIVE);
      currentSection = Section.OBJECTIVE;
      if (line.equalsIgnoreCase("max") || line.equalsIgnoreCase("maximize")
          || line.equalsIgnoreCase("maximise")) {
        LOGGER.debug("Found maximisation direction.");
        sense = OBJECTIVE_SENSE.MAX;
      } else if (line.equalsIgnoreCase("min")
          || line.equalsIgnoreCase("minimize")
           || line.equalsIgnoreCase("minimise")) {
        LOGGER.debug("Found minimisation direction.");
        sense = OBJECTIVE_SENSE.MIN;
      } else {
        throw new InputException(
            String.format("Line %d: unrecognised optimisation direction %s.", currentLine, line));
      }
      optDirectionRead = true;
    }
    // read objective name and objective expression
    String objName = "";
    final MutableMap<String, Double> coefficients = new UnifiedMap<>();
    while (true) {
      var line = getTrimmedLine(bufferedReader);
      final int colonIndex = line.indexOf(':');
      if (line.equalsIgnoreCase("subject to")
          || line.equalsIgnoreCase("such that")
          || line.equalsIgnoreCase("s.t.")
          || line.equalsIgnoreCase("st.")
          || line.equalsIgnoreCase("st")) {
        LOGGER.debug("Switching to section {}.", Section.CONSTRAINTS);
        currentSection = Section.CONSTRAINTS;
        break;
      } else if (colonIndex != -1) { // parsing objective name
        objName = getName(line, 0, colonIndex, currentLine);
        LOGGER.trace("Found objective name: {}.", objName);
        line = line.substring(colonIndex + 1).trim();
      }
      // parse objective expression
      LOGGER.trace("Parsing line {}: {}", currentLine, line);
      final var linComb = parseLinComb(variableBuilders, line, currentLine);
      linComb.forEachKeyValue((key, value) -> coefficients.merge(key, value, Double::sum));
    }
    return new Objective(objName, sense, coefficients.toImmutable());
  }

  ImmutableList<Constraint> readConstraints(
      final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders)
      throws IOException, InputException {
    if (currentSection != Section.CONSTRAINTS) {
      throw new InputException(String.format("Expected Section.CONSTRAINTS, found %s.", currentSection));
    }
    final MutableList<Constraint> constraints = ArrayListAdapter.newList();
    ConstraintBuilder consBuilder = null;
    while (true) {
      var line = getTrimmedLine(bufferedReader);
      final int colonIndex = line.indexOf(':');
      if (line.equalsIgnoreCase("end")) {
        LOGGER.debug("Switching to section {}.", Section.END);
        currentSection = Section.END;
        break;
      } else if (line.equalsIgnoreCase("bounds")) {
        LOGGER.debug("Switching to section {}.", Section.BOUNDS);
        currentSection = Section.BOUNDS;
        break;
      } else if (colonIndex != -1) { // constraint name found
        final var name = getName(line, 0, colonIndex, currentLine);
        LOGGER.trace("Found constraint name: {}.", name);
        consBuilder = new ConstraintBuilder().setName(name).setLineNumber(currentLine);
        line = line.substring(colonIndex + 1).trim();
      }
      LOGGER.trace("Parsing line {}: {}", currentLine, line);
      final var result = parseConstraintLine(line, variableBuilders);
      if (consBuilder == null) {
        throw new InputException("Constraint builder is null.");
      }
      if (result instanceof Unit) {
        final var lhs = parseLinComb(variableBuilders, (String) result.getValue(0), currentLine);
        consBuilder.mergeCoefficients(lhs);
      }
      else if (result instanceof Pair) {
        consBuilder.setSense((ConstraintSense) result.getValue(0));
        consBuilder.setRhs((Double) result.getValue(1));
        constraints.add(consBuilder.build());
        consBuilder = null;
      }
      else if (result instanceof Triplet) {
        final var lhs = parseLinComb(variableBuilders, (String) result.getValue(0), currentLine);
        consBuilder.mergeCoefficients(lhs);
        consBuilder.setSense((ConstraintSense) result.getValue(1));
        consBuilder.setRhs((Double) result.getValue(2));
        constraints.add(consBuilder.build());
        consBuilder = null;
      } else {
        throw new InputException("Unexpected constraint format.");
      }
    }
    return constraints.toImmutable();
  }

  private void readBounds(final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders) throws IOException, InputException {
    if (currentSection != Section.BOUNDS) {
      throw new InputException(String.format("Expected Section.BOUNDS, found %s.", currentSection));
    }
    while (true) {
      final var line = getTrimmedLine(bufferedReader);
      if (line.isBlank()) {
        continue;
      }
      else if (line.equalsIgnoreCase("end")) {
        LOGGER.debug("Switching to section {}.", Section.END);
        currentSection = Section.END;
        break;
      }
      LOGGER.trace("Parsing line {}: {}", currentLine, line);
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
          final var variable = getVariableBuilder(tokens[0].trim(), variableBuilders, currentLine);
          final var bound = parseValue(tokens[1].trim(), currentLine);
          variable.setLb(bound);
          variable.setUb(bound);
        } else {
          tokens = line.split("\s"); // split by whitespace character
          if (tokens.length != 2 || !tokens[1].equalsIgnoreCase("free")) {
            throw new InputException(String.format("Line %d: expected free variable expression, found %s.", currentLine, line));
          }
          final var variable = getVariableBuilder(tokens[0].trim(), variableBuilders, currentLine);
          LOGGER.trace("Parsing free variable {}.", tokens[0].trim());
          variable.setLb(Double.NEGATIVE_INFINITY);
          variable.setUb(Double.POSITIVE_INFINITY);
        }
      }
      case 2 -> { // var <= bound || bound <= var
        var variable = variableBuilders.get(tokens[0].trim());
        if (variable == null) {
          LOGGER.trace("Parsing one-sided lower bound.");
          variable = getVariableBuilder(tokens[1].trim(), variableBuilders, currentLine);
          final var bound = parseValue(tokens[0].trim(), currentLine);
          variable.setLb(bound);
        }
        else {
          LOGGER.trace("Parsing one-sided upper bound.");
          final var bound = parseValue(tokens[1].trim(), currentLine);
          variable.setUb(bound);
        }
      }
      case 3 -> { // lb <= var <= ub
        final var variable = getVariableBuilder(tokens[1].trim(), variableBuilders, currentLine);
        final var lb = parseValue(tokens[0].trim(), currentLine);
        final var ub = parseValue(tokens[2].trim(), currentLine);
        variable.setLb(lb);
        variable.setUb(ub);
      }
      default ->
        throw new InputException(String.format("Line %d: unknown bound format %s", currentLine, line));
    }
  }

  /**
   * Returns either Unit<String> or Pair<ConstraintSense, Double> or Triple<String, ConstraintSense, Double>
   */
  private Tuple parseConstraintLine(final String line, final MutableMap<String, VariableBuilder> variableBuilders) {
    final var sensePattern = Pattern.compile("[><=]{1,2}");
    final var senseMatcher = sensePattern.matcher(line);
    if (senseMatcher.find()) { // lhs sense rhs || sense rhs
      final var constraintSense = ConstraintSense.from(senseMatcher.group());
      LOGGER.trace("Found constraint sense: {}", constraintSense.representation);
      final var tokens = Arrays.stream(line.split(senseMatcher.group()))
          .filter(s -> !s.isBlank())
          .collect(Collectors.toUnmodifiableList());
      if (tokens.size() == 1) { // sense rhs
        final var rhs = parseValue(tokens.get(0), currentLine);
        return new Pair<>(constraintSense, rhs);
      }
      else if (tokens.size() == 2) { // lhs sense rhs
        final var rhs = parseValue(tokens.get(1), currentLine);
        return new Triplet<>(tokens.get(0), constraintSense, rhs);
      }
      else {
        throw new InputException(String.format("Line %d: invalid constraint line %s.", currentLine, line));
      }
    }
    else { // only lhs
      return new Unit<>(line.trim());
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
   * Returns the next line without leading space, trailing space, and trailing comment and increases
   * line counter
   */
  private String getTrimmedLine(final BufferedReader reader) throws IOException {
    if (!reader.ready()) {
      throw new InputException("Buffered reader is not ready.");
    }
    String line = reader.readLine();
    ++currentLine;
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
