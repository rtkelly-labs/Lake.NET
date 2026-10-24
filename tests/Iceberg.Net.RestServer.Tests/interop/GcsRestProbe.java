// Iceberg Java (RESTCatalog + its default FileIO) against the .NET REST server, for GcsCrossEngineTests.cs.
//
//   java -cp '<iceberg-core, iceberg-gcp and the GCS client jars>/*' GcsRestProbe.java \
//       <uri> <credential> <namespace> <table> <gcs service host>
//
// Loads the table with X-Iceberg-Access-Delegation: vended-credentials, then asks the table's FileIO whether a file
// under the table exists (and one outside it), so the GCS requests reach <gcs service host>, which records their
// Authorization headers. Prints one JSON line: the FileIO class and the vended token as the FileIO sees it.

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.rest.RESTCatalog;

public class GcsRestProbe {
  public static void main(String[] args) throws Exception {
    try (RESTCatalog catalog = new RESTCatalog()) {
      catalog.initialize(
          "dotnet",
          Map.of(
              "uri", args[0],
              "credential", args[1],
              "scope", "catalog",
              "header.X-Iceberg-Access-Delegation", "vended-credentials",
              "gcs.service.host", args[4],
              "gcs.project-id", "probe-project"));
      Table table = catalog.loadTable(TableIdentifier.of(Namespace.of(args[2]), args[3]));
      FileIO io = table.io();
      boolean inside = io.newInputFile(table.location() + "/metadata/probe.json").exists();
      String bucket = table.location().substring(0, table.location().indexOf('/', "gs://".length()));
      boolean outside = io.newInputFile(bucket + "/elsewhere/probe.json").exists();
      List<StorageCredential> credentials =
          io instanceof SupportsStorageCredentials s ? s.credentials() : List.of();
      String prefix = credentials.isEmpty() ? "" : credentials.get(0).prefix();
      String token = credentials.isEmpty() ? "" : credentials.get(0).config().get("gcs.oauth2.token");
      String expiresAt =
          credentials.isEmpty() ? "" : credentials.get(0).config().get("gcs.oauth2.token-expires-at");
      System.out.println(
          String.format(
              "@@PROBE@@{\"io\":\"%s\",\"credentials\":%d,\"prefix\":\"%s\",\"token\":\"%s\",\"expires_at\":\"%s\",\"inside\":%s,\"outside\":%s}",
              io.getClass().getName(), credentials.size(), prefix, token, expiresAt, inside, outside));
    }
  }
}
