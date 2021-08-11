package de.asbestian.jplex.input;

import de.asbestian.jplex.input.Constraint.ConstraintBuilder;
import de.asbestian.jplex.input.Constraint.ConstraintSense;
import de.asbestian.jplex.input.Objective.ObjectiveBuilder;
import de.asbestian.jplex.input.Objective.ObjectiveSense;
import de.asbestian.jplex.input.Variable.VariableBuilder;
import de.asbestian.jplex.input.Variable.VariableType;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
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
      return representation.stream();
    }

    Section(final ImmutableList<String> values) {
      representation = values;
    }

    private final ImmutableList<String> representation;
  }

  private static final BiPredicate<String, Section> sectionReached = (line, section) -> section.rep().anyMatch(line::equalsIgnoreCase);

  private static final BiPredicate<String, List<Section>> notReached = (line, list) -> list.stream().noneMatch(sec -> sectionReached.test(line, sec));

  private static final BiFunction<String, List<Section>, Section> getSection = (line, allowed) -> allowed.stream().filter(sec -> sectionReached.test(line, sec)).findFirst()
      .orElseThrow(() -> new InputException(String.format("No section found. Expected sections: %s", allowed)));

  private void ensureSection(final Section expected) {
    if (currentSection != expected) {
      throw new InputException(String.format("Expected %s, found %s.", expected, currentSection));
    }
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
    currentSection = Section.START;
    currentLineNumber = 0;
    final MutableMap<String, VariableBuilder> variableBuilders = new UnifiedMap<>();
    try (final BufferedReader bf = new BufferedReader(new FileReader(path))) {
      final var objectiveSense = readObjectiveSense(bf);
      objectives = readObjectives(bf, variableBuilders, objectiveSense);
      constraints = readConstraints(bf, variableBuilders);
      while(currentSection != Section.END) {
        switch (currentSection) {
          case BOUNDS -> readBounds(bf, variableBuilders);
          case BINARY -> readBinary(bf, variableBuilders);
          case GENERAL -> readGeneral(bf, variableBuilders);
          default -> throw new InputException(String.format("Unexpected section: %s", currentSection));
        }
      }
      variables = variableBuilders.collectValues((key, value) -> value.build()).toImmutable();
    } catch (final IOException | InputException e) {
      LOGGER.error("Problem reading section {} in input file {}", currentSection, path);
      LOGGER.error(e.getMessage());
      objectives = Lists.immutable.empty();
      constraints = Lists.immutable.empty();
      variables = Maps.immutable.empty();
    }
  }

  public Objective getObjective(final int index) {
    return objectives.get(index);
  }

  public int getNumberOfObjectives() { return objectives.size(); }

  public int getNumberOfVariables() {
    return variables.size();
  }

  public int getNumberOfConstraints() {
    return constraints.size();
  }

  public List<Variable> getContinuousVariables() { return getVariablesWithType(VariableType.CONTINUOUS); }

  public List<Variable> getIntegerVariables() { return getVariablesWithType(VariableType.INTEGER); }

  public List<Variable> getBinaryVariables() { return getVariablesWithType(VariableType.BINARY); }

  private List<Variable> getVariablesWithType(final VariableType type) {
    return variables.select(var -> var.type().equals(type)).stream().toList();
  }

  private ObjectiveSense readObjectiveSense(final BufferedReader bufferedReader) throws IOException, InputException {
    ensureSection(Section.START);
    final var line = getNextProperLine(bufferedReader);
    final var isMax = ObjectiveSense.MAX.rep().anyMatch(line::equalsIgnoreCase);
    final var isMin = ObjectiveSense.MIN.rep().anyMatch(line::equalsIgnoreCase);
    if (isMax || isMin) {
      currentSection = Section.OBJECTIVE;
      LOGGER.debug("Switching to section {}.", currentSection);
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
          String.format("Line %d: unrecognised optimisation direction %s.", currentLineNumber, line));
    }
  }

  private ImmutableList<Objective> readObjectives(
      final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders,
      final ObjectiveSense objectiveSense)
      throws IOException, InputException {
    ensureSection(Section.OBJECTIVE);
    final MutableList<ObjectiveBuilder> builders = Lists.mutable.empty();
    String line = getNextProperLine(bufferedReader);
    while(!sectionReached.test(line, Section.CONSTRAINTS)) {
      final int colonIndex = line.indexOf(':');
      if (colonIndex != -1) { // objective function name found; add new builder
        final var name = getName(line, 0, colonIndex, currentLineNumber);
        LOGGER.trace("Found objective name: {}.", name);
        final var builder = new ObjectiveBuilder().setSense(objectiveSense).setName(name);
        builders.add(builder);
        line = line.substring(colonIndex + 1).trim();
      }
      LOGGER.trace("Parsing line {}: {}", currentLineNumber, line);
      final var linComb = parseLinComb(variableBuilders, line, currentLineNumber);
      builders.getLast().mergeCoefficients(linComb);
      line = getNextProperLine(bufferedReader);
    }
    currentSection = Section.CONSTRAINTS;
    LOGGER.debug("Switching to section {}.", currentSection);
    return builders.collect(ObjectiveBuilder::build).toImmutable();
  }

  ImmutableList<Constraint> readConstraints(
      final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders)
      throws IOException, InputException {
    ensureSection(Section.CONSTRAINTS);
    final MutableList<Constraint> constraints = Lists.mutable.empty();
    ConstraintBuilder consBuilder = null;
    final var sections = List.of(Section.BOUNDS, Section.BINARY, Section.GENERAL, Section.END);
    var line = getNextProperLine(bufferedReader);
    while (notReached.test(line, sections)) {
      final int colonIndex = line.indexOf(':');
      if (colonIndex != -1) { // constraint name found
        final var name = getName(line, 0, colonIndex, currentLineNumber);
        LOGGER.trace("Found constraint name: {}.", name);
        consBuilder = new ConstraintBuilder().setName(name).setLineNumber(currentLineNumber);
        line = line.substring(colonIndex + 1).trim();
      }
      LOGGER.trace("Parsing line {}: {}", currentLineNumber, line);
      final var result = parseConstraintLine(line, variableBuilders);
      if (consBuilder == null) {
        throw new InputException("Constraint builder is null.");
      }
      switch (result) {
        case Unit unit -> {
          final var lhs = parseLinComb(variableBuilders, unit.line(), currentLineNumber);
          consBuilder.mergeCoefficients(lhs);
        }
        case Pair pair -> {
          consBuilder.setSense(pair.sense);
          consBuilder.setRhs(pair.rhs);
          constraints.add(consBuilder.build());
          consBuilder = null;
        }
        case Triple triple -> {
          final var lhs = parseLinComb(variableBuilders, triple.line(), currentLineNumber);
          consBuilder.mergeCoefficients(lhs);
          consBuilder.setSense(triple.sense);
          consBuilder.setRhs(triple.rhs);
          constraints.add(consBuilder.build());
          consBuilder = null;
        }
      }
      line = getNextProperLine(bufferedReader);
    }
    currentSection = getSection.apply(line, sections);
    LOGGER.debug("Switching to section {}.", currentSection);
    return constraints.toImmutable();
}

  private void readBounds(final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders) throws IOException, InputException {
    ensureSection(Section.BOUNDS);
    final var sections = List.of(Section.BINARY, Section.GENERAL, Section.END);
    var line = getNextProperLine(bufferedReader);
    while (notReached.test(line, sections)) {
      LOGGER.trace("Parsing line {}: {}", currentLineNumber, line);
      parseBound(line, variableBuilders);
      line = getNextProperLine(bufferedReader);
    }
    currentSection = getSection.apply(line, sections);
    LOGGER.debug("Switching to section {}.", currentSection);
  }

  private void readType(final BufferedReader bufferedReader, final MutableMap<String, VariableBuilder> variableBuilders,
      final Section expected, final List<Section> allowed, final VariableType type) throws IOException {
    ensureSection(expected);
    var line = getNextProperLine(bufferedReader);
    while(notReached.test(line, allowed)) {
      final var names = line.split("\\s");
      for (final var name : names) {
        variableBuilders.get(name).setType(type);
      }
      line = getNextProperLine(bufferedReader);
    }
    currentSection = getSection.apply(line, allowed);
    LOGGER.debug("Switching to section {}.", currentSection);
  }


  private void readBinary(final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders) throws IOException {
    readType(bufferedReader, variableBuilders, Section.BINARY, List.of(Section.GENERAL, Section.END), VariableType.BINARY);
  }

  private void readGeneral(final BufferedReader bufferedReader,
      final MutableMap<String, VariableBuilder> variableBuilders) throws IOException {
    readType(bufferedReader, variableBuilders, Section.GENERAL, List.of(Section.BINARY, Section.END), VariableType.INTEGER);
  }

  private static double parseValue(final String expr, final int currentLine) {
    if (Stream.of("inf", "+inf", "infinity", "+infinity").anyMatch(expr::equalsIgnoreCase)) {
      return Double.POSITIVE_INFINITY;
    }
    else if (Stream.of("-inf", "-infinity").anyMatch(expr::equalsIgnoreCase)) {
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
              currentLineNumber);
          final var bound = parseValue(tokens[1].trim(), currentLineNumber);
          variable.setLb(bound);
          variable.setUb(bound);
        } else {
          tokens = line.split("\s"); // split by whitespace character
          if (tokens.length != 2 || !tokens[1].equalsIgnoreCase("free")) {
            throw new InputException(String.format("Line %d: expected free variable expression, found %s.",
                currentLineNumber, line));
          }
          final var variable = getVariableBuilder(tokens[0].trim(), variableBuilders,
              currentLineNumber);
          LOGGER.trace("Parsing free variable {}.", tokens[0].trim());
          variable.setLb(Double.NEGATIVE_INFINITY);
          variable.setUb(Double.POSITIVE_INFINITY);
        }
      }
      case 2 -> { // var <= bound || bound <= var
        var variable = variableBuilders.get(tokens[0].trim());
        if (variable == null) {
          LOGGER.trace("Parsing one-sided lower bound.");
          variable = getVariableBuilder(tokens[1].trim(), variableBuilders, currentLineNumber);
          final var bound = parseValue(tokens[0].trim(), currentLineNumber);
          variable.setLb(bound);
        }
        else {
          LOGGER.trace("Parsing one-sided upper bound.");
          final var bound = parseValue(tokens[1].trim(), currentLineNumber);
          variable.setUb(bound);
        }
      }
      case 3 -> { // lb <= var <= ub
        final var variable = getVariableBuilder(tokens[1].trim(), variableBuilders,
            currentLineNumber);
        final var lb = parseValue(tokens[0].trim(), currentLineNumber);
        final var ub = parseValue(tokens[2].trim(), currentLineNumber);
        variable.setLb(lb);
        variable.setUb(ub);
      }
      default ->
        throw new InputException(String.format("Line %d: unknown bound format %s", currentLineNumber, line));
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
        final var rhs = parseValue(tokens.get(0), currentLineNumber);
        return new Pair(constraintSense, rhs);
      }
      else if (tokens.size() == 2) { // lhs sense rhs
        final var rhs = parseValue(tokens.get(1), currentLineNumber);
        return new Triple(tokens.get(0), constraintSense, rhs);
      }
      else {
        throw new InputException(String.format("Line %d: invalid constraint line %s.",
            currentLineNumber, line));
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
      ++currentLineNumber;
      final int commentBegin = line.indexOf('\\');
      if (commentBegin != -1) {
        line = line.substring(0, commentBegin);
      }
    } while (line.isBlank());
    return line.strip();
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
