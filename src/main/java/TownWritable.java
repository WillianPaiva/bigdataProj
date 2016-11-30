
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author alaguitard
 */
class TownWritable implements Writable{

    private Text country,city;
    private IntWritable region;
    private LongWritable population;

    private DoubleWritable latitude;
    private DoubleWritable longitude;
    
    public TownWritable(String line) {
        String[] splitted = line.split(",");
        this.country = new Text(splitted[0]);
        this.city = new Text(splitted[1]);
        this.region = new IntWritable(Integer.parseInt(splitted[3]));
        this.population = new LongWritable(Long.parseLong(splitted[4]));
        this.latitude = new DoubleWritable(Double.parseDouble(splitted[5]));
        this.longitude = new DoubleWritable(Double.parseDouble(splitted[6]));
    }
    
    public TownWritable()
    {
        this.country = new Text();
        this.city = new Text();
        this.region = new IntWritable();
        this.population = new LongWritable();
        this.latitude = new DoubleWritable();
        this.longitude = new DoubleWritable();
    }

    public Text getCountry() {
        return country;
    }

    public void setCountry(Text country) {
        this.country = country;
    }

    public Text getCity() {
        return city;
    }

    public void setCity(Text city) {
        this.city = city;
    }

    public IntWritable getRegion() {
        return region;
    }

    public void setRegion(IntWritable region) {
        this.region = region;
    }

    public LongWritable getPopulation() {
        return population;
    }

    public void setPopulation(LongWritable population) {
        this.population = population;
    }

    public DoubleWritable getLatitude() {
        return latitude;
    }

    public void setLatitude(DoubleWritable latitude) {
        this.latitude = latitude;
    }

    public DoubleWritable getLongitude() {
        return longitude;
    }

    public void setLongitude(DoubleWritable longitude) {
        this.longitude = longitude;
    }
    
    public void write(DataOutput d) throws IOException {
        country.write(d);
        city.write(d);
        region.write(d);
        population.write(d);
        latitude.write(d);
        longitude.write(d);
    }

    public void readFields(DataInput di) throws IOException {
        country.readFields(di);
        city.readFields(di);
        region.readFields(di);
        population.readFields(di);
        latitude.readFields(di);
        longitude.readFields(di);
    }
    
}
