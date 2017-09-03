import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogRegExp  {
public static void main(String argv[]) {

        String logEntryPattern =
       "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
        System.out.println("RE Pattern:");
        System.out.println(logEntryPattern);

        System.out.println("Input line is:");
        String logEntryLine = "66.249.69.97 - - [24/Sep/2014:22:25:44 +0000] \"GET /071300/242153 HTTP/1.1\" 404 514 \"-\" \"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)\"";

        System.out.println(logEntryLine);
        
        Pattern p = Pattern.compile(logEntryPattern);
        Matcher matcher = p.matcher(logEntryLine);
        if (!matcher.find()) {
            System.out.println( "Cannot parse logline .................. " );     
        }
        boolean found = p.matcher(logEntryLine).lookingAt();

        System.out.println("'" + logEntryPattern + "'" +
            (found ? " matches '" : " doesn't match '") + logEntryLine + "'");
        
        System.out.println("IP Address: " + matcher.group(1));
        System.out.println("clientIdentd: " + matcher.group(2));
        System.out.println("userID: " + matcher.group(3));
        System.out.println("dateTimeString: " + matcher.group(4));
        System.out.println("method: " + matcher.group(5));
        System.out.println("endpoint: " + matcher.group(6));
        System.out.println("protocol: " + matcher.group(7));
        System.out.println("responseCode: " + matcher.group(8));
        System.out.println("contentSize: " + matcher.group(9));
    }
}