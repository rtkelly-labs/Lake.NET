// Iceberg Java's REST scan-planning wire format as an oracle for the .NET client (see docs/regression-suite.md).
// Runs with Iceberg Java on the classpath (the Spark image's iceberg-spark-runtime jar, or the REST fixture image's
// adapter jar) via the JDK's single-file source launcher; make-fixtures.sh and TestSupport/JavaOracle.cs run it. Commands, one JSON document on stdout:
//   plan <metadata.json> [filter-json]  Java plans the scan itself (table.newScan().filter(...).planFiles()) and
//                                       prints the PlanTableScanResponse a REST server would send
//   expr <expression-json>              parse with ExpressionParser: {"string": toString(), "json": toJson()}
//   request <request-json>              parse with PlanTableScanRequestParser and print it re-serialized
//   plans <cases.json>                  many "plan" calls in one JVM: [{"metadata","filter"?}] -> [response, ...]
//   exprs <expressions.json>            many "expr" calls in one JVM: [expression, ...] -> [{"string","json"}, ...]
import java.util.*;
import org.apache.iceberg.*;
import org.apache.iceberg.expressions.*;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.rest.PlanStatus;
import org.apache.iceberg.rest.requests.PlanTableScanRequestParser;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponseParser;

public class ScanPlanOracle {
  public static void main(String[] args) throws Exception {
    switch (args[0]) {
      case "plan" -> System.out.println(plan(args[1], args.length > 2 ? args[2] : null));
      case "expr" -> {
        Expression e = ExpressionParser.fromJson(args[1]);
        System.out.println("{\"string\":" + quote(e.toString()) + ",\"json\":" + ExpressionParser.toJson(e) + "}");
      }
      case "plans", "exprs" -> {
        var cases = new com.fasterxml.jackson.databind.ObjectMapper().readTree(new java.io.File(args[1]));
        var out = new StringBuilder("[");
        for (var c : cases) {
          if (out.length() > 1) out.append(",");
          if (args[0].equals("plans")) out.append(plan(c.get("metadata").asText(), c.hasNonNull("filter") ? c.get("filter").toString() : null));
          else {
            Expression e = ExpressionParser.fromJson(c.toString());
            out.append("{\"string\":").append(quote(e.toString())).append(",\"json\":").append(ExpressionParser.toJson(e)).append("}");
          }
        }
        System.out.println(out.append("]"));
      }
      case "request" -> System.out.println(PlanTableScanRequestParser.toJson(PlanTableScanRequestParser.fromJson(args[1])));
      default -> throw new IllegalArgumentException(args[0]);
    }
  }

  static String plan(String metadataLocation, String filter) throws Exception {
    var io = new HadoopFileIO(new org.apache.hadoop.conf.Configuration());
    Table table = new BaseTable(new StaticTableOperations(metadataLocation, io), "oracle");
    TableScan scan = table.newScan();
    if (filter != null) scan = scan.filter(ExpressionParser.fromJson(filter));
    List<FileScanTask> tasks = new ArrayList<>();
    Map<String, DeleteFile> deletes = new LinkedHashMap<>();
    try (CloseableIterable<FileScanTask> planned = scan.planFiles()) {
      for (FileScanTask task : planned) {
        tasks.add(task);
        // Deletion vectors of many data files share one Puffin file: a DV is its file plus its blob offset.
        for (DeleteFile d : task.deletes()) deletes.putIfAbsent(d.location() + "#" + d.contentOffset(), d);
      }
    }
    var builder = PlanTableScanResponse.builder().withPlanStatus(PlanStatus.COMPLETED);
    builder.withFileScanTasks(tasks);
    // 1.10 takes the delete files and specs explicitly; 1.12 derives both from the tasks.
    invokeIfPresent(builder, "withDeleteFiles", List.class, new ArrayList<>(deletes.values()));
    invokeIfPresent(builder, "withSpecsById", Map.class, table.specs());
    PlanTableScanResponse response = builder.build();
    return PlanTableScanResponseParser.toJson(response);
  }

  static void invokeIfPresent(Object target, String method, Class<?> type, Object arg) throws Exception {
    for (var c = target.getClass(); c != null; c = c.getSuperclass()) {
      try {
        var m = c.getDeclaredMethod(method, type); // 1.12 keeps withSpecsById package-private
        m.setAccessible(true);
        m.invoke(target, arg);
        return;
      } catch (NoSuchMethodException e) {
        // not on this class, or not in this version
      }
    }
  }

  static String quote(String s) {
    return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
  }
}
