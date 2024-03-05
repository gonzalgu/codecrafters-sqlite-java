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
        Assertions.assertEquals("mango", result.getTable());
        Assertions.assertEquals("watermelon", result.getColumns().get(0));
    }


    @Test
    void parseQueryWithWhere(){
        String sql = "SELECT name, color FROM apples WHERE color = 'Yellow'";
        var result = Query.parse(sql);
        System.out.println(result);
    }
}