import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** An entry of Apache access log. */
case class ApacheAccessLog(ipAddress: String,
                           clientIdentd: String,
                           userId: String,
                           dateTime: String,
                           method: String,
                           endpoint: String,
                           protocol: String,
                           responseCode: Int,
                           contentSize: Long)

object ApacheAccessLog {
  val PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)"

  /**
   * Parse log entry from a string.
   *
   * @param log A string, typically a line from a log file
   * @return An entry of Apache access log
   * @throws RuntimeException Unable to parse the string
   */
  def parseLogLine(log: String): ApacheAccessLog = {
    val p = Pattern.compile(PATTERN);
    val m = p.matcher(log);
    if (!m.find()) {
      println("Cannot parse logline > " + log);
    }
    val found = p.matcher(log).lookingAt();

    ApacheAccessLog(m.group(1), m.group(2), m.group(3), m.group(4),
      m.group(5), m.group(6), m.group(7), m.group(8).toInt, m.group(9).toLong);
  }
}