import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class BtreePageTest {

    @Test
    void testReadPage() throws IOException {
//        int pageNumber = 4;
//        String databaseFilePath = "sample.db";
//        ByteBuffer fileContents = ByteBuffer
//                .wrap(Files.readAllBytes(Path.of(databaseFilePath)))
//                .order(ByteOrder.BIG_ENDIAN);
//
//        BtreePage page = BtreePage.readPage(fileContents, pageNumber);
//        System.out.println(page.btreePageHeader.cellCounts);
//        ByteBuffer pageContents = ByteBuffer.wrap(page.pageContents).order(ByteOrder.BIG_ENDIAN);
//        for(var cellOffset : page.cellPointerArray){
//            pageContents.position(cellOffset);
//            var cell = Cell.readCell(pageContents, page.btreePageHeader.pageType);
//            ByteBuffer cellPayload = ByteBuffer.wrap(cell.getPayload()).order(ByteOrder.BIG_ENDIAN);
//            var record = Record.readRecord(cellPayload);
//            var numCols = record.serialTypes.size();
//            for(int i=0;i<numCols;++i){
//                System.out.printf("type: %d, value: %s |", record.serialTypes.get(i), String.valueOf(record.values.get(i)));
//            }
//            System.out.println();
//        }
    }

}