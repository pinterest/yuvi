package deltadebug;

import java.util.LinkedList;
import java.util.List;

/**
 * A simple Java implementation of Andreas Zeller's Delta Debugging algorithm
 *
 * Reference: https://www.st.cs.uni-saarland.de/whyprogramsfail/code/dd/DD.java
 *
 * @author Ben Holland
 *
 * NOTE: This code is copied from: https://github.com/benjholla/ddmin
 */
public class DeltaDebug {

  /**
   * Given an input that causes a failing/error condition, this implementation of a divide and conquer
   * algorithm splits the input data into smaller chunks and checks if the smaller input reproduces
   * the failing/error condition with a smaller input.

   * @param input The pre-chunked test input to reduce
   * @param harness A test harness implementation that returns true (pass) or false (fail/error) for a given input
   *
   * @return A minimized input that reproduces the failing/error condition
   */
  public static <E> List<E> ddmin(List<E> input, TestHarness<E> harness) {

    int n = 2;
    while (input.size() >= 2) {
      List<List<E>> subsets = split(input, n);
      boolean complementFailing = false;

      for (List<E> subset : subsets){
        List<E> complement = difference(input, subset);
        if (harness.run(complement) == TestHarness.FAIL) {
          input = complement;
          n = Math.max(n - 1, 2);
          complementFailing = true;
          break;
        }
      }

      if (!complementFailing) {
        if (n == input.size()) {
          break;
        }

        // increase set granularity
        n = Math.min(n * 2, input.size());
      }
    }

    return input;
  }

  /**
   * Returns the difference of set a and set b
   * @param a
   * @param b
   * @return
   */
  private static <E> List<E> difference(List<E> a, List<E> b) {
    List<E> result = new LinkedList<E>();
    result.addAll(a);
    result.removeAll(b);
    return result;
  }

  /**
   * Splits the input set s into n subsets
   * @param s
   * @param n
   * @return
   */
  private static <E> List<List<E>> split(List<E> s, int n) {
    List<List<E>> subsets = new LinkedList<List<E>>();
    int position = 0;
    for (int i = 0; i < n; i++) {
      List<E> subset = s.subList(position, position + (s.size() - position) / (n - i));
      subsets.add(subset);
      position += subset.size();
    }
    return subsets;
  }

}
