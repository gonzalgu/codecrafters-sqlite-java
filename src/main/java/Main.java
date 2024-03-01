import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class Main {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Missing <database path> and <command>");
            return;
        }

        String databaseFilePath = args[0];
        String command = args[1];

        switch (command) {
            case ".dbinfo" -> {
                try {
                    printDbInfo(databaseFilePath);
                } catch (IOException e) {
                    System.out.println("Error reading file: " + e.getMessage());
                }
            }
            case ".tables" -> {
                try {
                    printTables(databaseFilePath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            default -> System.out.println("Missing or invalid command passed: " + command);
        }
    }



    private static void printDbInfo(String databaseFilePath) throws IOException {
        ByteBuffer fileContents = getContents(databaseFilePath);
        // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order.
        // '& 0xFFFF' is used to convert the signed short to an unsigned int.
        int pageSize = fileContents.position(16).getShort() & 0xFFFF;

        // Uncomment this block to pass the first stage
        System.out.printf("database page size: %d\n", pageSize);
        ByteBuffer firstPage = fileContents.position(100);
        var pageHeader = BtreePageHeader.getHeader(firstPage);
        //leaf b-tree page
        assert pageHeader.pageType == 0x0d;
        System.out.printf("number of tables: %d\n", pageHeader.cellCounts);
    }


    private static void printTables(String databaseFilePath) throws IOException {
        ByteBuffer fileContents = getContents(databaseFilePath).position(100);
        ByteBuffer firstPage = fileContents.position(100);
        var pageHeader = BtreePageHeader.getHeader(firstPage);
        firstPage.position(pageHeader.getGetStartOfCellContentArea());

        List<List<String>> records = new Stack<>();
        for(int i=0;i<pageHeader.cellCounts; ++i){
            records.add(printCell(firstPage));
        }
        Collections.reverse(records);
        System.out.println(String.join(" ", records.stream().map(rec -> rec.get(2)).toList()));
    }

    private static ByteBuffer getContents(String databaseFilePath) throws IOException {
        ByteBuffer fileContents = ByteBuffer
                .wrap(Files.readAllBytes(Path.of(databaseFilePath)))
                .order(ByteOrder.BIG_ENDIAN);
        return fileContents;
    }

    private static List<String> printCell(ByteBuffer cellArray){
        //varint
        VarInt bytesOfPayload = Cell.from(cellArray);
        //varint
        VarInt rowId = Cell.from(cellArray);
        //payload
        byte[] payload = new byte[(int)bytesOfPayload.value()];
        cellArray.get(payload);

        ByteBuffer recordBuf = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);

        return printRecord(recordBuf);
        //if everything fits in the page, omit this.
        //int firstPageOverflowList = cellArray.getInt();
    }

    private static List<String> printRecord(ByteBuffer buffer){
        //header
        var sizeOfHeader = Cell.from(buffer);
//        System.out.printf("sizeOfHeader: %s\n", sizeOfHeader);
        long remaining = sizeOfHeader.value() - sizeOfHeader.bytesRead();
        List<Integer> columnsType = new ArrayList<>();
        while(remaining > 0){
            var varInt = Cell.from(buffer);
            columnsType.add((int)varInt.value());
            remaining -= varInt.bytesRead();
        }
//        System.out.printf("number of columns: %d\n", columnsType.size());
        List<String> values = new ArrayList<>();
        for(var colType : columnsType){
            switch (colType){
                case 0:
                    values.add("NULL");
                    break;
                case 1:
                    values.add(String.valueOf(buffer.get()));
                    break;
                case 2:
                    values.add(String.valueOf(buffer.getShort()));
                    break;
                case 3:
                    throw new RuntimeException("not implemented");

                case 4:
                    values.add(String.valueOf(buffer.getInt()));
                    break;
                case 5:
                    throw new RuntimeException("not implemented");

                case 6:
                    values.add(String.valueOf(buffer.getLong()));
                    break;

                case 7:
                    values.add(String.valueOf(buffer.getFloat()));
                    break;

                case 8:
                    values.add("0");
                    break;

                case 9:
                    values.add("1");
                    break;

                default:{
                    int contentSize = 0;
                    if(colType >= 12 && colType % 2 == 0){
                        contentSize = (colType - 12)/2;
                    }
                    if(colType >= 13 && colType % 2 == 1){
                        contentSize = (colType - 13)/2;
                    }

                    if(contentSize > 0){
                        byte[] contents = new byte[contentSize];
                        buffer.get(contents);
                        values.add(new String(contents));
                    }else{
                        values.add("");
                    }
                }
            }
        }
        //right now only print table names.
        //3rd element of each record.
        //System.out.printf("%s ", values.get(2));
        return values;
        //System.out.printf("values: %s\n", String.join(", ", values));
    }
}




