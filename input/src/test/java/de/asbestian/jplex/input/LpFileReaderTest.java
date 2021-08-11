package de.asbestian.jplex.input;

import static org.junit.jupiter.api.Assertions.assertEquals;

import de.asbestian.jplex.input.Objective.ObjectiveSense;
import de.asbestian.jplex.input.Variable.VariableType;
import org.junit.jupiter.api.Test;

/** @author Sebastian Schenker */
class LpFileReaderTest {

  @Test
  void threeObjectivesTwoConstraints_onlyContinuousVariables() {
    final var path = "src/test/resources/3obj_2cons.lp";

    final LpFileReader input = new LpFileReader(path);

    assertEquals(3, input.getNumberOfObjectives());
    assertEquals(3, input.getNumberOfVariables());
    assertEquals(2, input.getNumberOfConstraints());
    assertEquals(3, input.getContinuousVariables().size());
    assertEquals(0, input.getIntegerVariables().size());
    assertEquals(0, input.getBinaryVariables().size());
  }

  @Test
  void noEndSection_empty() {
    final var path = "src/test/resources/no_end_section.lp";

    final LpFileReader input = new LpFileReader(path);

    assertEquals(0, input.getNumberOfObjectives());
    assertEquals(0, input.getNumberOfVariables());
    assertEquals(0, input.getNumberOfConstraints());
  }

  @Test
  void twoObjectivesTwoConstraints_onlyBinaryVariables() {
    final var path = "src/test/resources/2obj_2cons_only_binary_vars.lp";

    final LpFileReader input = new LpFileReader(path);

    assertEquals(2, input.getNumberOfObjectives());
    assertEquals(3, input.getNumberOfVariables());
    assertEquals(2, input.getNumberOfConstraints());
    assertEquals(0, input.getContinuousVariables().size());
    assertEquals(0, input.getIntegerVariables().size());
    assertEquals(3, input.getBinaryVariables().size());
  }

  @Test
  void twoObjectivesTwoConstraints_allVariableTypes() {
    final var path = "src/test/resources/2obj_2cons_all_variable_types.lp";

    final LpFileReader input = new LpFileReader(path);

    assertEquals(2, input.getNumberOfObjectives());
    assertEquals(3, input.getNumberOfVariables());
    assertEquals(2, input.getNumberOfConstraints());
    assertEquals(1, input.getContinuousVariables().size());
    assertEquals(1, input.getIntegerVariables().size());
    assertEquals(1, input.getBinaryVariables().size());
  }

  @Test
  void oneObjectiveTwoConstraints_allVariablesWithBounds() {
    final var path = "src/test/resources/1obj_1cons_all_variables_with_bounds.lp";

    final LpFileReader input = new LpFileReader(path);
    final var z = input.getBinaryVariables().get(0);
    final var x = input.getContinuousVariables().get(0);
    final var y = input.getIntegerVariables().get(0);
    final var objective = input.getObjective(0);

    assertEquals(1, input.getNumberOfObjectives());
    assertEquals(3, input.getNumberOfVariables());
    assertEquals(1, input.getNumberOfConstraints());
    assertEquals(1, input.getContinuousVariables().size());
    assertEquals(1, input.getIntegerVariables().size());
    assertEquals(1, input.getBinaryVariables().size());
    assertEquals("x", x.name());
    assertEquals(Double.NEGATIVE_INFINITY, x.lb());
    assertEquals(Double.POSITIVE_INFINITY, x.ub());
    assertEquals(VariableType.CONTINUOUS, x.type());
    assertEquals("y", y.name());
    assertEquals(10., y.lb());
    assertEquals(12, y.ub());
    assertEquals(VariableType.INTEGER, y.type());
    assertEquals("z", z.name());
    assertEquals(0, z.lb());
    assertEquals(1, z.ub());
    assertEquals(VariableType.BINARY, z.type());
    assertEquals(ObjectiveSense.MAX, objective.sense());
    assertEquals(-2, objective.getCoeff(x));
    assertEquals(-3, objective.getCoeff(y));
    assertEquals(4, objective.getCoeff(z));
  }
}
