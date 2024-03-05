import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryTest {
    @Test
    void testParseQuery(){
        var query = "select c1, c2, c3 from table";
        var result = Query.parse(query);
        Assertions.assertEquals("table", result.getTable());
        Assertions.assertEquals(3, result.getColumns().size());
        List<String> expected = List.of("c1", "c2", "c3");
        var it1 = result.getColumns().iterator();
        var it2 = expected.iterator();
        while(it1.hasNext() && it2.hasNext()){
            Assertions.assertEquals(it2.next(), it1.next());
        }
    }

    @Test
    void parseCase(){
        var query = "select watermelon from mango";
        var result = Query.parse(query);
    }

    @Test
    void parseColumns(){
        String tableDef = "CREATE TABLE banana (id integer primary key, strawberry text,vanilla text,pistachio text,coconut text,banana text)";

    }
}