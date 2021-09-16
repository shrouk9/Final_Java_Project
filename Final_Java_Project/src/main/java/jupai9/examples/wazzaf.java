/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jupai9.examples;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.common.record.RecordMetaData;
import com.univocity.parsers.conversions.Conversion;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;
import static scala.collection.immutable.StringLike$class.r;


/**
 *
 * @author ahmed
 */
public class wazzaf {
    public static void main(String[] args) throws IOException {
            wazzaf obj = new wazzaf();

            final SparkSession sparkSession = SparkSession.builder ().appName ("AirBnb Analysis Demo").master ("local[6]")
                .getOrCreate ();
        // Get DataFrameReader using SparkSession
            final DataFrameReader dataFrameReader = sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
            dataFrameReader.option ("header", "true");
        // reading data into WazzafDF variable
            Dataset<Row> WazzafDF = dataFrameReader.csv ("src/main/resources/Wuzzuf_Jobs.csv");
        // print some of data
            //WazzafDF.show(20);
        // Display structure and summary of the data
            WazzafDF.describe().show();
            WazzafDF.printSchema();
        //Clean the data (null, duplications)
            //WazzafDF= obj.cleanData(WazzafDF);
        // Count the jobs for each company and display that in order (What are the most demanding companies for jobs?)
            WazzafDF.createOrReplaceTempView("WAZZAF_DATA");
            System.out.println("------------------------------------------------------------//");
            /*final Dataset<Row> numOfJobs =sparkSession.sql("SELECT Company, "
                    + "count(Title) as Numofjobs "
                    + "FROM WAZZAF_DATA "
                    + "Group by Company order by Numofjobs desc") ;*/
            //numOfJobs.show(10);
            //numOfJobs.collectAsList().stream().forEach(System.out::println);
            //obj.piegraph(numOfJobs,2,20);
            /*final Dataset<Row> JobTitles =sparkSession.sql("SELECT Title, "
                    + "count(Title) as NumofTitles "
                    + "FROM WAZZAF_DATA "
                    + "Group by Title order by NumofTitles desc") ;*/
            //JobTitles.show(50);
            //obj.bargraph(JobTitles, "Title", "NumofTitles",1,10);
            /*final Dataset<Row> mostPopularAreas =sparkSession.sql("SELECT Location, "
                    + "count(Location) as MPA "
                    + "FROM WAZZAF_DATA "
                    + "Group by Location order by MPA desc") ;*/
            //mostPopularAreas.show(50);
            //obj.bargraph(mostPopularAreas, "Location", "MPA",1,10);
            //final Dataset<Row> Skills =sparkSession.sql("SELECT distinct(Skills) , count(Skills) as SkillsCount FROM WAZZAF_DATA group by Skills order by SkillsCount desc ") ;
            //Skills.show(50);
            WazzafDF.createOrReplaceTempView ("S");
            sparkSession.sql("select distinct(skill) , count(LOWER(skill)) as No_of_skills  FROM  (SELECT explode(split(skills, ',') ) AS `skill` FROM WAZZAF_DATA) group by (skill) order by (No_of_skills)  desc ").show(50);
            List<String> skii = new ArrayList<String>();
            skii = obj.readSkillsCSV();
            obj.writeSkills(skii);
            Dataset<Row> Skills = dataFrameReader.csv ("src/main/resources/skills.CSV");
            Skills.createOrReplaceTempView("SKILLS_DATA");
            final Dataset<Row> Skillscount =sparkSession.sql("SELECT distinct(LOWER(Skills)) , count(Skills) as SkillsCount FROM SKILLS_DATA group by Skills order by SkillsCount desc ") ;
            Skillscount.show(50);
        }
        public List<String> readSkillsCSV() {
            List<String> skills = new ArrayList<String>();
        try {
            BufferedReader br = new BufferedReader(new FileReader ("src/main/resources/Wuzzuf_Jobs.csv"));
            String line= br.readLine();
            do 
            {
                line = br.readLine();
                if (line != null)
                {
                    String[] attributes = line.split(",");
                    
                    for (int i = 7 ; i < attributes.length ; ++i)
                    {
                        skills.add(attributes[i].replaceAll("\"",""));
                    }   
                }                              
            } while (line != null);
            br.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(String.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(String.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return skills ;
    }
        public void writeSkills(List<String> skills) throws IOException{
            FileWriter mohamed = null;
        try {
            mohamed = new FileWriter("src/main/resources/skills.CSV");
        } catch (IOException ex) {
            Logger.getLogger(wazzaf.class.getName()).log(Level.SEVERE, null, ex);
        }
            BufferedWriter file = new BufferedWriter(mohamed); 
            file.write("Skills\n");
            for (String p : skills)
        {
            file.write(p.trim() +'\n');
        }
            file.close();
        }
        public Dataset<Row> cleanData (Dataset<Row> data) {
            data = data.withColumn ("Title", data.col ("Title").cast ("String"))
                .filter (data.col ("Title").isNotNull ());
            data = data.withColumn ("Company", data.col ("Company").cast ("String"))
                .filter (data.col ("Company").isNotNull ());
            data = data.withColumn ("Level", data.col ("Level").cast ("String"))
                .filter (data.col ("Level").isNotNull ());
            data = data.withColumn ("Skills", data.col ("Skills").cast ("String"))
                .filter (data.col ("Skills").isNotNull ());
            data = data.withColumn ("Location", data.col ("Location").cast ("String"))
                .filter (data.col ("Location").isNotNull ());
            data = data.withColumn ("Type", data.col ("Type").cast ("String"))
                .filter (data.col ("Type").isNotNull ());
            data = data.withColumn ("YearsExp", data.col ("YearsExp").cast ("String"))
                .filter (data.col ("YearsExp").isNotNull ());
            data = data.withColumn ("Country", data.col ("Country").cast ("String"))
                .filter (data.col ("Country").isNotNull ());
            data = data.distinct();
            return data;
            }
        public void piegraph (Dataset<Row> df,int start , int end) {
        List<Float> yData = df.select("Numofjobs").as(Encoders.FLOAT()).collectAsList();
        List<String> xData = df.select("Company").as(Encoders.STRING()).collectAsList();
        List<Float> yresult = new ArrayList<Float>();
        List<String> xresult = new ArrayList<String>();
        int ylistSize = yData.size();
        int xlistSize = xData.size();
        for (int i = 0; i < ylistSize; ++i) {
            // accessing each element of array
            yresult.add(yData.get(i));
            xresult.add(xData.get(i));
            };

        PieChart chart = new PieChartBuilder().width(800).height(800).title("jobs_for_each_company").build();
//chart.addSeries ("First Class", xresult.get(1));
        for (int i = start-1 ; i < start+end-1; ++i) {
            // accessing each element of array
            chart.addSeries (xresult.get(i), yresult.get(i));
        };

// Show it
        new SwingWrapper(chart).displayChart();
    
    }
        public void bargraph (Dataset<Row> df,String x,String y,int start,int end) {
        List<Float> yData = df.select(y).as(Encoders.FLOAT()).collectAsList();
        List<String> xData = df.select(x).as(Encoders.STRING()).collectAsList();
        List<Float> yresult = new ArrayList<Float>();
        List<String> xresult = new ArrayList<String>();
        int ylistSize = yData.size();
        int xlistSize = xData.size();
        for (int i = start-1; i < end+start-1; ++i) {
            // accessing each element of array
            yresult.add(yData.get(i));
            xresult.add(xData.get(i));
            };

        CategoryChart chart = new CategoryChartBuilder ().width (1024).height (768).build ();
        // Customize Chart
        chart.getStyler ().setLegendPosition (Styler.LegendPosition.InsideNW);
        chart.getStyler ().setHasAnnotations (true);
        chart.getStyler ().setStacked (true);
        // Series
        chart.addSeries ("fdsfsdfs", xresult , yresult);
        // Show it
        new SwingWrapper (chart).displayChart ();
    
    }
     
}
