package com.fahasa.fpointprocess;

import com.fahasa.com.fpointprocess.dao.DAO;
import static com.fahasa.fpointprocess.FPointProcess.FVIP_LEVEL_RULE_SQL;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main process that trigger upgrade level for fpoint
 * @author Thang Pham
 */
public class UpgradeLevelProcess {
    
    private static DAO dao;
    private Map<Integer, Object> fVipMap = new HashMap<>();
    private static final Logger logger = Logger.getLogger(UpgradeLevelProcess.class.getName());
    
    private static final String VIP_AND_FVIP_LIST_SQL = "select entity_id, email, fpoint, \n" +
"fpoint_accure_year, vip_level, num_freeship from fhs_customer_entity ce\n" +
"where ce.fpoint_accure_year >= 30000";
    
    private static final String UPGRADE_VIP_LEVEL = "update fhs_customer_entity \n" +
"set vip_level={{NEW_VIP_LEVEL}},\n" +
"num_freeship={{NEW_FREESHIP}}\n" +
"where entity_id={{CUSTOMER_ID}}";
    
    private static final String UPGRADE_LEVEL_ACTION_LOG = "insert into fhs_purchase_action_log\n" +
"(account, customer_id, action, value, amountAfter, updateBy, lastUpdated, description, amountBefore, type)\n" +
"values('{{EMAIL}}', {{CUSTOMER_ID}}, 'upgrade level', {{VALUE}}, {{LEVEL_AFTER}}, \n" +
"'tp_script', now(), '{{DESC}}', {{LEVEL_BEFORE}}, 'vip level');";
    
    private static final String REWARD_MORE_FREESHIP_FOR_UPGRADE_LEVEL_ACTION_LOG = 
            "insert into fhs_purchase_action_log\n" +
"(account, customer_id, action, value, amountAfter, updateBy, lastUpdated, description, amountBefore, type)\n" +
"values('{{EMAIL}}', {{CUSTOMER_ID}}, 'add more freeship', {{VALUE}}, {{FREESHIP_AFTER}}, \n" +
"'tp_script', now(), '{{DESC}}', {{FREESHIP_BEFORE}}, 'freeship');";
    
    private static final Integer VIP_LEVEL_ID = 1;
    private static final Integer FVIP_LEVEL_ID = 2;
    
    public static void main(String[] args){
        dao = new DAO();
        UpgradeLevelProcess upgradeLevel = new UpgradeLevelProcess();
        upgradeLevel.upgradeLevelMainProcess();
    }
    
    public void upgradeLevelMainProcess(){
        //Build FVIP Level Rule Map
        List<Object> r = dao.runNativeQuery(FVIP_LEVEL_RULE_SQL);
        int numPointBeginForVip = -1;
        int numPointBeginForFVip = -1;
        for(Object o : r){
            Object[] data = (Object[]) o;
            Integer fVipId = (Integer)data[0];
            if(fVipId == 1){
                //This VIP
                numPointBeginForVip = (Integer) data[5];
            }else if(fVipId == 2){
                //This FVIP
                numPointBeginForFVip = (Integer) data[5];
            }
            fVipMap.put(fVipId, o);
        }
        //This is list of customer, who will get upgrade level this time
        List<Object> vipList = dao.runNativeQuery(VIP_AND_FVIP_LIST_SQL);
        logger.log(Level.INFO, "There are totals {0} accounts to be upgrade.", vipList.size());
        for(Object o : vipList){
            Object[] data = (Object[]) o;
            Long customerId = (Long) data[0];
            String customerEmail = (String) data[1];
            int fPoint = ((BigDecimal) data[2]).intValue();
            int fPointAccureYear = (Integer) data[3];
            int currentVipLevel = (Integer) data[4];
            int currentNumFreeShip = (Integer) data[5];
            int newVipLevel = currentVipLevel;
            int newNumFreeShip = currentNumFreeShip;
//            customerEmail = "phamtn8@gmail.com";
//            customerId = Long.valueOf(18129);
            if(fPointAccureYear < numPointBeginForFVip){
                //This is VIP Level
                newVipLevel = VIP_LEVEL_ID;                
            }else{
                //This is FVIP
                newVipLevel = FVIP_LEVEL_ID;
            }
            //Check if there is an upgrade in level
            if(newVipLevel > currentVipLevel){
                logger.log(Level.INFO, "Processing {0} to upgrade from level {1} to level {2}", 
                        new Object[]{customerEmail, currentVipLevel, newVipLevel});
                Object[] x = (Object[])fVipMap.get(newVipLevel);
                //New level, reward more freeship
                newNumFreeShip += (Integer) x[3];
                String upgradeLevelSql = UPGRADE_VIP_LEVEL
                        .replace("{{NEW_VIP_LEVEL}}", String.valueOf(newVipLevel))
                        .replace("{{NEW_FREESHIP}}", String.valueOf(newNumFreeShip))
                        .replace("{{CUSTOMER_ID}}", String.valueOf(customerId));

                String upgradeLevelActionLogSql = UPGRADE_LEVEL_ACTION_LOG
                        .replace("{{EMAIL}}", customerEmail)
                        .replace("{{CUSTOMER_ID}}", String.valueOf(customerId))
                        .replace("{{VALUE}}", String.valueOf(newVipLevel - currentVipLevel))
                        .replace("{{DESC}}", "Upgrade level. Current fpoint_accure_year: " + fPointAccureYear)
                        .replace("{{LEVEL_AFTER}}", String.valueOf(newVipLevel))
                        .replace("{{LEVEL_BEFORE}}", String.valueOf(currentVipLevel));

                String rewardMoreFreeshipForLevelUpgradeActionLog = REWARD_MORE_FREESHIP_FOR_UPGRADE_LEVEL_ACTION_LOG
                        .replace("{{EMAIL}}", customerEmail)
                        .replace("{{CUSTOMER_ID}}", String.valueOf(customerId))
                        .replace("{{VALUE}}", String.valueOf(newNumFreeShip - currentNumFreeShip))
                        .replace("{{FREESHIP_AFTER}}", String.valueOf(newNumFreeShip))
                        .replace("{{DESC}}", "Add more num freeship when fvip upgrade: Current fpoint_accure_year: " + fPointAccureYear)
                        .replace("{{FREESHIP_BEFORE}}", String.valueOf(currentNumFreeShip));
                dao.upgradeFVipLevel(upgradeLevelSql, upgradeLevelActionLogSql, 
                        rewardMoreFreeshipForLevelUpgradeActionLog);
            }
        }
    }
}
