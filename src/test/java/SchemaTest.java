import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class SchemaTest {
    @Test
    void loadSchema() throws IOException {
        String databaseFilePath = "sample.db";
        ByteBuffer fileContents = ByteBuffer
                .wrap(Files.readAllBytes(Path.of(databaseFilePath)))
                .order(ByteOrder.BIG_ENDIAN);
        //read first page
        BtreePage page = BtreePage.readPage(fileContents, 1);
        var schema = Schema.loadSchema(page, "apples");
        Assertions.assertEquals("apples", schema.tableName);
    }

    @Test
    void parseTableDefinition(){
        String tableDefinition = "CREATE TABLE banana (id integer primary key, strawberry text,vanilla text,pistachio text,coconut text,banana text)";
        var result = Schema.parseColumns(tableDefinition);
        System.out.println(result);
    }

}