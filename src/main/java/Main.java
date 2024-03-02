import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Missing <database path> and <command>");
            return;
        }

        String databaseFilePath = args[0];
        String command = args[1];
        DB db = new DB(databaseFilePath);

        switch (command) {
            case ".dbinfo" -> {
                try {
                    db.load();
                    var dbInfo = db.dbInfo();
                    System.out.println(dbInfo);
                } catch (IOException e) {
                    System.out.println("Error reading file: " + e.getMessage());
                }
            }
            case ".tables" -> {
                try {
                    db.load();
                    db.printTableNames();
                    //printTables(databaseFilePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> {
                var queryTokens = command.split(" ");
                if(queryTokens.length == 4){
                    var tableName = queryTokens[3];
                    try {
                        db.load();
                        var c = db.countRows(tableName);
                        System.out.println(c);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }else{
                    throw new RuntimeException("invalid command");
                }
            }
        }
    }
}




