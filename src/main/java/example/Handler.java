package example;


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.CellAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import request.Employee;
import response.Response;

// Handler value: example.Handler
public class Handler implements RequestHandler<S3Event, String> {
  Gson gson = new GsonBuilder().setPrettyPrinting().create();
  private static final Logger logger = LoggerFactory.getLogger(Handler.class);
  private DynamoDB dynamoDb;
  private String DYNAMO_DB_TABLE_NAME = "customer_details";
  private Regions REGION = Regions.US_EAST_1;

  @Override
  public String handleRequest(S3Event s3event, Context context) {
    try {
      logger.info("EVENT: " + gson.toJson(s3event));
      S3EventNotificationRecord record = s3event.getRecords().get(0);
      
      String srcBucket = record.getS3().getBucket().getName();

      // Object key may have spaces or unicode non-ASCII characters.
      String srcKey = record.getS3().getObject().getUrlDecodedKey();

      String dstBucket = srcBucket;
      String dstKey = "resized-" + srcKey;

      // Download the excel from S3 into a stream
      AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
      S3Object s3Object = s3Client.getObject(new GetObjectRequest(
              srcBucket, srcKey));
      InputStream objectData = s3Object.getObjectContent();

      // Parsing the excel sheet
      Workbook workbook = null;
      try {
        workbook = WorkbookFactory.create(objectData);

      } catch (InvalidFormatException | IOException e) {
        e.printStackTrace();
      }
      Iterator<org.apache.poi.ss.usermodel.Row> rowIterator = workbook.getSheetAt(0).rowIterator();

      List<Employee> employeeList= new ArrayList<>();
      while (rowIterator.hasNext()) {

        Employee employee = new Employee();

        Row currentRow = rowIterator.next();

        // don't read the header
        if (currentRow.getRowNum() == 0) {
          continue;
        }

        Iterator<Cell> cellIterator = currentRow.iterator();

        while (cellIterator.hasNext()) {

          Cell currentCell = cellIterator.next();
          CellAddress address = currentCell.getAddress();

          if (0 == address.getColumn()) {
             logger.info("(int) currentCell.getNumericCellValue(): " + (int) currentCell.getNumericCellValue());
            employee.setId((int) currentCell.getNumericCellValue());
           } else if (1 == address.getColumn()) {
             logger.info("currentCell.getStringCellValue(): " + currentCell.getStringCellValue());
            employee.setFirstName(currentCell.getStringCellValue());
          } else if (2 == address.getColumn()) {
            // 3rd col is "Color"
            employee.setLastName(currentCell.getStringCellValue());
         }

          employeeList.add(employee);

        }
        workbook.close();

      }
      logger.info("employeeList size: " + employeeList.size());
      this.initDynamoDbClient();
      for (Employee emp : employeeList) {
        this.persistData(emp);
      }
      Response personResponse = new Response();
      personResponse.setMessage("Message Saved Successfully");
      logger.info("Parsing Done " );
      return "Ok";
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void initDynamoDbClient() {
    AmazonDynamoDBClient client = new AmazonDynamoDBClient();
    client.setRegion(Region.getRegion(REGION));
    this.dynamoDb = new DynamoDB(client);
  }

  private PutItemOutcome persistData(Employee employee) {
    Table table = dynamoDb.getTable(DYNAMO_DB_TABLE_NAME);
    PutItemOutcome outcome = table.putItem(new PutItemSpec().withItem(
            new Item().withNumber("id", employee.getId())
                    .withString("firstName", employee.getFirstName())
                    .withString("lastName", employee.getLastName())));
    return outcome;
  }
}
