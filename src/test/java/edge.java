/**
 * @ClassName edge
 * @Description TODO
 * @Autor yanni
 * @Date 2020/6/22 16:19
 * @Version 1.0
 **/
public class edge {

    private String source;
    private String target;
    private String edgeType = "PROJECTION";

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getEdgeType() {
        return edgeType;
    }

    public void setEdgeType(String edgeType) {
        this.edgeType = edgeType;
    }
}
