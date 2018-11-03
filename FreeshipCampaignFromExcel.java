
package com.fahasa.fpointprocess;

import com.fahasa.com.fpointprocess.dao.DAO;
import com.fahasa.com.fpointprocess.utils.Utils;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

/**
 * Read and reward freeship campaign from excel
 * @author Thang Pham
 */
public class FreeshipCampaignFromExcel {
    
    private static DAO dao;
    private static final int START_ROW = 2;
    private static final int EMAIL_COL_INDEX = 3;
    private static final Logger logger = Logger.getLogger(FreeshipCampaignFromExcel.class.getName());
    public static final String GET_CUSTOMER_SQL = "select ce.fpoint, ce.vip_level, ce.num_freeship, \n"
            + "ce.fpoint_accure_year, ce.num_rewarded_freeship, ce.email, ce.entity_id \n"
            + "from fhs_customer_entity ce \n"
            + "where ce.email='{{EMAIL}}'";
    private static final String CAMPAIGN_DESC = "add more num freeship for campaign dh > 700K ngày 11 - 12/05";
    
    public static void main(String[] args){
        String excelPath = args[0];
        dao = new DAO();
        FreeshipCampaignFromExcel promotion = new FreeshipCampaignFromExcel();
        promotion.mainPromotionProcess(excelPath);
    }
    
    public void mainPromotionProcess(String excelPath){
        try (FileInputStream file = new FileInputStream(new File(excelPath))) {
            Workbook wb = WorkbookFactory.create(file);
            Sheet sheet = wb.getSheetAt(0);
            Iterator<Row> rowIterator = sheet.iterator();            
            int rowCnt = 0;
            while (rowIterator.hasNext()) {            
                rowCnt++;                
                Row row = rowIterator.next();
                if(rowCnt <= START_ROW){
                    continue;
                }
                String customerEmail = Utils.getCellValueAsString(row.getCell(EMAIL_COL_INDEX))
                        .replace(" ", "").toLowerCase(); 
                List<Object> customers = dao.runNativeQuery(GET_CUSTOMER_SQL.replace("{{EMAIL}}", customerEmail));
                for(Object o : customers){
                    Object[] customer = (Object[]) o;
                    Integer numCurrentFreeShip = (Integer)customer[2];
                    Long customerId = (Long) customer[6];
                    Integer nextToBeRewardFreeship = numCurrentFreeShip + 1;
                    
                    String numFreeshipAddActionLogInsertQuery = "insert into fhs_purchase_action_log (account, customer_id,\n"
        + "action, value, amountAfter, updateBy, lastUpdated, order_id, suborder_id, description, \n"
        + "amountBefore, type) values ('{{EMAIL}}', {{CUSTOMER_ID}} , 'add more freeship', 1, {{NUMFREESHIPAFTER}}, \n"
        + "'tp script', now(), '', '', '{{DESC}}', \n"
        + "{{NUMFREESHIPBEFORE}}, 'freeship');";
                    numFreeshipAddActionLogInsertQuery = numFreeshipAddActionLogInsertQuery
                        .replace("{{EMAIL}}", customerEmail)
                        .replace("{{CUSTOMER_ID}}", String.valueOf(customerId))
                        .replace("{{DESC}}", CAMPAIGN_DESC)
                        .replace("{{NUMFREESHIPAFTER}}", String.valueOf(nextToBeRewardFreeship))                        
                        .replace("{{NUMFREESHIPBEFORE}}", String.valueOf(numCurrentFreeShip));
                    
                    //update number of freeship to user account
                    String updateNumFreeshipAccount = "update fhs_customer_entity \n" +
            "set num_freeship={{NUM_FREESHIP}} \n" +
            "where email='{{EMAIL}}'";
                    updateNumFreeshipAccount = updateNumFreeshipAccount
                            .replace("{{NUM_FREESHIP}}", String.valueOf(nextToBeRewardFreeship))
                            .replace("{{EMAIL}}", customerEmail);
                    dao.updateFreeshipOneTime(numFreeshipAddActionLogInsertQuery, updateNumFreeshipAccount);
                    
                    break;  //Should only be one customer per email
                }
            }
        } catch (IOException | InvalidFormatException | EncryptedDocumentException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
    }
}
