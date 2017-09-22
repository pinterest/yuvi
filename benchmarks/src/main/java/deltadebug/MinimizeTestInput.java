package deltadebug;

import com.pinterest.yuvi.chunk.ChunkManagerDataIntegrityChecker;

import deltadebug.DeltaDebug;
import deltadebug.TestHarness;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class MinimizeTestInput {

  private static TestHarness<String> harness = new TestHarness<String>(){
    @Override
    public int run(List<String> input) {
      // ChunkManagerDataIntegrityChecker checker = new ChunkManagerDataIntegrityChecker();
      // int[] result = checker.checkMetrics(input);
      if (2 == 1){
        return FAIL;
      }
      return PASS;
    }
  };

  public static void main(String[] args) throws IOException {
    Path path = Paths.get("/tmp/data_input");
    List<String> input = Files.lines(path, Charset.defaultCharset())
        .collect(Collectors.toList());

    System.out.println(DeltaDebug.ddmin(input, harness));
  }
}
