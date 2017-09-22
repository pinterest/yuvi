package deltadebug;

import java.util.List;

/**
 * An interface to implement a test harness for evaluating input sets
 * for passing or failing/error conditions.
 *
 * @author Ben Holland
 */
public abstract class TestHarness<E> {

  public static final int PASS = 1;
  public static final int UNRESOLVED = 0;
  public static final int FAIL = -1;

  /**
   * Returns true if the test passes and false if the test fails
   * @param <E>
   * @param complement
   * @return
   */
  public abstract int run(List<E> input);

}
