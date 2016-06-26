/**
 * Created by jack on 16-6-24.
 */
public class ss {
    private static ss ourInstance = new ss();

    public static ss getInstance() {
        return ourInstance;
    }

    private ss() {
    }
}
