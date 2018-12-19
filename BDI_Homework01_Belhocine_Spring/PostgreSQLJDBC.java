import org.fluttercode.datafactory.impl.DataFactory;

import java.sql.*;
import java.util.Date;

public class PostgreSQLJDBC {
    static Connection c = null;

    public static void main(String args[]) {
        try {
            Class.forName("org.postgresql.Driver");
            DataFactory df = new DataFactory();
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/postgres");
            c.setAutoCommit(false);


           for(int i = 1; i < 1001; i++) {
               //Paper Table Entry
               String paperID = Integer.toString(i);
               String paperTitle = (df.getName() + " " + df.getRandomWord() + " " + df.getRandomWord()).replaceAll("'", " ");
               String abstractText = df.getRandomText(200, 250);

               generateEntry("Paper", "(paperID, title, abstract)",
                     "(" + paperID + ", '" + paperTitle + "', '" + abstractText + "')");


               //Author Table Entries
               String authorID = Integer.toString(i);
               String author = df.getFirstName() + " " + df.getLastName();
               String authorName = author.replaceAll("'", " ");
               String mail = df.getEmailAddress().replaceAll("'", "");
               String[] affil = {"Department of Biology" , "Department of Chemistry", "Department of Computer Science", "Department of Physics", "Department of Silly Hats"};
               int j = df.getNumberBetween(0, 5);
               String affilitation = (affil[j]);

               generateEntry("Author", "(authorID, name, email, affilitation)",
                      "(" + authorID + ", '" + authorName + "', '" + mail + "', '" + affilitation + "')");

               //Conference Table
               String confID = Integer.toString(i);
               String confName = ("Symposium " + df.getRandomWord() + " " + df.getRandomWord()).replaceAll("'", " ");
               String ranking = Integer.toString(i);

               generateEntry("Conference ", "(confID, name, ranking)",
                     "(" + confID + ", '" + confName + "', " + ranking + ")");

               //Writes
               generateEntry("Writes", "(authorID, paperID)", "(" + authorID + ", " + paperID + ")");

               //Submites
               boolean[] accepted = {false, true};
               boolean isAccepted = accepted[df.getNumberBetween(0, 2)];
               Date date = df.getDateBetween(df.getDate(2000, 1, 1), df.getDate(2018, 9, 30));
               java.sql.Date sqlDate = new java.sql.Date(date.getTime());

               generateEntry("Submits", "(paperID, confID, isAccepted, date)", "(" + paperID + ", " + confID
                       + ", " + isAccepted + ", '" + sqlDate + "')");


           }
           for(int i = 1; i < 1001; i++) {
               //cites
               int paperIDfrom = df.getNumberBetween(1, 1001);
               int paperIDto = df.getNumberBetween(1, 1001);
               while (paperIDfrom == paperIDto) {
                   paperIDto = df.getNumberBetween(1, 1001);
               }
               generateEntry("Cites", "(paperIDfrom, paperIDto)", "(" + paperIDfrom + ", " + paperIDto + ")");
           }

            c.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
    }

    private static void generateEntry(String tableName, String fieldNames, String fieldValues) throws SQLException {
        Statement stmt;
        stmt = c.createStatement();
        String entry = "INSERT INTO " + tableName + " " + fieldNames + " VALUES " + fieldValues + ";";

        stmt.executeUpdate(entry);
        stmt.close();
        c.commit();
    }








}
