package ed.inf.adbs.lightdb;
import java.util.List;
import java.util.ArrayList;

/**
 * The Tuple class represents a row of data.
 */
public class Tuple {
    private List<Integer> attrs;

    // tuple constructor(for select)
    public Tuple(String tuple) {
        this.attrs = new ArrayList<>();
        if (tuple != null) {
            // ex: 1, 2, 3 => [1,2,3]
            String[] attrs = tuple.trim().split(",");
            for (String attr : attrs) {
                if (attr !=null && !attr.equals(" ")) {
                    this.attrs.add(Integer.parseInt(attr.trim()));
                }
                else {
                    this.attrs.add(null);
                }
            }
        }
    }
    // constructor for project
    public Tuple(List<Integer> attrs) {
        this.attrs = new ArrayList<>(attrs);
    }
    /**
     * ex: [100, 200, 300] => 100, 200, 300
     * @return
     */
    @Override
    public String toString() {
        if (attrs == null || attrs.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < attrs.size(); i++) {
            sb.append(attrs.get(i));
            if (i < attrs.size() - 1) sb.append(",");
        }
        return sb.toString();
    }

    /**
     * get tuple's attrs
     * @return
     */
    public List<Integer> getAttrs() {
        return attrs;
    }

    /**
     * get value of the specific index of the tuple
     * @param index
     * @return value of the index of the tuple
     */
    public int getKeyValue(int index) {
        if (index >= 0 && index < attrs.size()) {
            return attrs.get(index);
        }
        throw new IndexOutOfBoundsException("Tuple index out of range: " + index);
    }
}