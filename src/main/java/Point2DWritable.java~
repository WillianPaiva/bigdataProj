import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class Point2DWritable implements Writable{
    private Point2D.Double point;
    
    public Point2DWritable(){
    	this.point = new Point2D.Double();
    }

    public Point2DWritable(double x, double y){
        this.point = new Point2D.Double(x,y);
    }

    public Point2DWritable(Point2D.Double p){
        this.point = p;
    }

    public void write(DataOutput out) throws IOException
    {
        out.writeDouble(point.getX());
        out.writeDouble(point.getY());
    }

    public void readFields(DataInput in) throws IOException
    {
        point.setLocation(in.readDouble(),in.readDouble());
    }

    public Point2D.Double getPoint(){
        return this.point;
    }

    public void setPoint(Point2D.Double p){
        this.point = p;
    }

    public void setPoint(double x, double y){
        this.point = new Point2D.Double(x,y);
    }
    
    @Override
    public String toString()
    {
    	return this.point.toString();
    }
}
