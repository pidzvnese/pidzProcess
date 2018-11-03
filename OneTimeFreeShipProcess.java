
package com.fahasa.fpointprocess;

import com.fahasa.com.fpointprocess.dao.DAO;
import java.math.BigDecimal;
import java.util.List;

/**
 * Process 1 time all customer who have FPoint accure year for Freeship
 * @author Thang Pham
 */
public class OneTimeFreeShipProcess {
    
    public static String GET_ALL_CUSTOMER_WHO_HAS_FPOINT = "select ce.fpoint, ce.vip_level, ce.num_freeship, \n"
            + "ce.fpoint_accure_year, ce.num_rewarded_freeship, ce.email from fhs_customer_entity ce \n"
            + "where ce.fpoint_accure_year > 0";
    private static DAO dao;
    
    public static void main(String[] args){
        dao = new DAO();
        OneTimeFreeShipProcess freeShipProcess = new OneTimeFreeShipProcess();
        freeShipProcess.mainController();
    }
    
    public void mainController(){
        List<Object> customerList = dao.runNativeQuery(GET_ALL_CUSTOMER_WHO_HAS_FPOINT);
        for(Object o: customerList){
            Object[] customerData = (Object[]) o;
            Integer customerCurrentFPoint = ((BigDecimal)customerData[0]).intValue();
            Integer currentFVipLevel = (Integer) customerData[1];
            Integer numCurrentFreeShip = (Integer) customerData[2];
            Integer currentFpoitnAccrYear = (Integer) customerData[3];
            Integer numRewardedFreeship = (Integer) customerData[4];
            String customerEmail = (String) customerData[5];
            int numFreeToBeReward = (int)Math.floor(currentFpoitnAccrYear / FPointProcess.FREESHIP_STEP_SIZE);
            if(numFreeToBeReward > 0){              
                //numRewardedFreeship is freshly query num_rewarded_freeship from db
                //so substract from it to see if we need to reward this current customer
                numFreeToBeReward = numFreeToBeReward - numRewardedFreeship;
                if(numFreeToBeReward > 0){
                    //Reward 1 more Freeship, and update num_rewarded_freeship
                    int nextToBeRewardFreeship = numCurrentFreeShip + numFreeToBeReward; 
                    //action log for reward freeship
                    String numFreeshipAddActionLogInsertQuery = "insert into fhs_purchase_action_log (account, \n"
        + "action, value, amountAfter, updateBy, lastUpdated, order_id, suborder_id, description, \n"
        + "amountBefore, type) values ('{{EMAIL}}', 'add more freeship', {{NUMFREESHIP}}, {{NUMFREESHIPAFTER}}, \n"
        + "'tp script', now(), '', '', 'add more num freeship per 1.000.000 vnd', \n"
        + "{{NUMFREESHIPBEFORE}}, 'freeship');";
                    numFreeshipAddActionLogInsertQuery = numFreeshipAddActionLogInsertQuery
                        .replace("{{EMAIL}}", customerEmail)
                        .replace("{{NUMFREESHIP}}", String.valueOf(numFreeToBeReward))
                        .replace("{{NUMFREESHIPAFTER}}", String.valueOf(nextToBeRewardFreeship))                        
                        .replace("{{NUMFREESHIPBEFORE}}", String.valueOf(numCurrentFreeShip));
                    //update number of freeship to user account
                    String updateNumFreeshipAccount = "update fhs_customer_entity \n" +
"set num_freeship={{NUM_FREESHIP}}, \n" +
"num_rewarded_freeship={{NUM_REWARDED_FREESHIP}}\n" +
"where email='{{EMAIL}}'";
                    updateNumFreeshipAccount = updateNumFreeshipAccount
                            .replace("{{NUM_FREESHIP}}", String.valueOf(nextToBeRewardFreeship))
                            .replace("{{NUM_REWARDED_FREESHIP}}", 
                                    String.valueOf(numFreeToBeReward + numRewardedFreeship))
                            .replace("{{EMAIL}}", customerEmail);
                    dao.updateFreeshipOneTime(numFreeshipAddActionLogInsertQuery, updateNumFreeshipAccount);
                }
            }            
        }
    }
}
