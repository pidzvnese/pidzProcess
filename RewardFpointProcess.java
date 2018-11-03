/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.fahasa.fpointprocess;

import com.fahasa.com.fpointprocess.dao.DAO;
import com.fahasa.com.fpointprocess.utils.Utils;
import com.fahasa.fpointprocess.dto.CustomerFpointInfo;
import com.fahasa.fpointprocess.model.Action;
import com.fahasa.fpointprocess.model.Variables;
import static com.squareup.okhttp.internal.Internal.logger;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author phongnh92
 */
public class RewardFpointProcess {

    private static DAO dao;

    public static final Integer MAX_VALUE = 10000000;

    public static final String TYPE_REWARD = "rewardFpoint";
    public static final String EXCLUDE_FILTER_SKU_DEFAULT = "1903041244305a,1903041244312a,codedientu100000,phieuquatang60000";
    //6152: phieu qua tang
    //4388: teaching resource
    //5421: dictionary
    public static final String EXCLUDE_FILTER_CATEGORY_MID_DEFAULT = "4388,5421,6152";

    public static final String ACTION_ADD = "add";
    public static final String ACTION_MUL = "multiply";
    public static final String ACTION_INCLUDE = "include";
    public static final String ACTION_EXCLUDE = "exclude";

    private Map<Integer, Object> fVipMap = new HashMap<>();

    public static int SILVER_LEVEL = 0;
    public static int GOLD_LEVEL = 1;
    public static int DIAMOND_LEVEL = 2;
    public static final int FREESHIP_STEP_SIZE = 10000; //Equal value of 1.000.000 vnd
    //Mean max nhan diem khong the vuot qua so nay
    public static final Integer MAX_FPOINT_NHAN_DIEM = 5;
    public final static String[] ESCALATION_EMAIL_LIST = new String[]{
        "phamtn8@gmail.com",
        "trung.qn.1010@gmail.com"
    };
    public static String FVIP_LEVEL_RULE_SQL = "select entity_id, label, percent_point_collect, \n"
            + "num_freeship_award, num_plastic_wrap_reward, num_point_begin_level, num_point_end_level, \n"
            + "value_reward_for_birthday from fhs_fvip_level_rule";

    public static String CURRENT_FPOINT_REWARD_RULE = "select\n"
            + "  a.action_name,\n"
            + "  a.type        as typeRule,\n"
            + "  a.action,\n"
            + "  a.value,\n"
            + "  a.max_value   as maxRewardedValue,\n"
            + "  r.id          as ruleId,\n"
            + "  r.campaign_id as campaignId,\n"
            + "  c.from_date,\n"
            + "  c.to_date,\n"
            + "  f.type        as typeFilter,\n"
            + "  f.value,\n"
            + "  f.action as actionFilter\n"
            + "from fhs_fpoint_campaign_action a\n"
            + "  join fhs_fpoint_campaign_rule r on r.action_id = a.id\n"
            + "  join fhs_fpoint_campaign_rule_filter f on f.rule_id = r.id\n"
            + "  join fhs_fpoint_campaign c on r.campaign_id = c.id\n"
            + "where c.end_status = 0\n"
            + "order by f.rule_id desc";

    public static String ADD_ACTION_LOG_QUERY = "insert into fhs_purchase_action_log (account, customer_id, \n"
            + "action, campaignId, campaign_rule_id, value,  amountAfter, updateBy, lastUpdated, order_id, suborder_id, description, \n"
            + "amountBefore, type) values ('{{EMAIL}}', {{CUSTOMER_ID}}, '{{ACTION}}', {{REWARD_ID}}, {{RULE_ID}}, {{NUM}}, {{NUMAFTER}}, \n"
            + "'tp script', now(), {{ORDERID}},'','{{DESC}}', \n"
            + "{{NUMBEFORE}}, '{{REWARDTYPE}}');";

    //There should be MAX 1 campaign for each order id
    public static String CHECK_EXIST_ORDER_REWARDED = "select count(1)\n"
            + "from fhs_purchase_action_log\n"
            + "where order_id = {{ORDERID}} and campaignId > 0\n";

    public static String GET_CUSTOMER_ENTITY = "select ce.fpoint, ce.vip_level, ce.num_freeship,\n"
            + "ce.fpoint_accure_year, ce.num_rewarded_freeship from fhs_customer_entity ce \n"
            + "where ce.entity_id={{CUSTOMER_ID}}";

    public static String GET_RULE_FILTER = "select type, action,min_value, max_value, value\n"
            + "from fhs_fpoint_campaign_rule_filter\n"
            + "where rule_id = {{RULE_ID}}";

    public static String FPOINT_CUSTOMER_UPDATE = "update fhs_customer_entity set fpoint={{NUMFPOINT}} \n"
            + "where entity_id={{CUSTOMER_ID}}";

    public static String FREESHIP_CUSTOMER_UPDATE = "update fhs_customer_entity set num_freeship={{NUMFREESHIP}} \n"
            + "where entity_id={{CUSTOMER_ID}}";

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger LOGGER = Logger.getLogger(RewardFpointProcess.class.getName());

    public static void main(String[] args) {
        dao = new DAO();
        RewardFpointProcess reward = new RewardFpointProcess();
        reward.rewardFpointProcess();
    }

    public void insertLogAndUpdateCE(CustomerFpointInfo cf, double valueRewarded,
            double nextToBeReward, String logMessage, String promotionType) {
        dao = new DAO();
        String numFpointAddActionLogInsertQuery = null;
        String updateFpointCustomerQuery = null;

        String insertLog = ADD_ACTION_LOG_QUERY
                .replace("{{EMAIL}}", cf.getCustomerEmail())
                .replace("{{CUSTOMER_ID}}", String.valueOf(cf.getCustomerId()))
                .replace("{{ACTION}}", cf.getActionName())
                .replace("{{REWARD_ID}}", String.valueOf(cf.getCampaignId()))
                .replace("{{RULE_ID}}", String.valueOf(cf.getRuleId()))
                .replace("{{NUM}}", String.valueOf(valueRewarded))
                .replace("{{NUMAFTER}}", String.valueOf(nextToBeReward))
                .replace("{{ORDERID}}", cf.getOrderId())
                .replace("{{DESC}}", logMessage)
                .replace("{{REWARDTYPE}}", promotionType);

        if (promotionType.equals(Variables.TYPE_FREESHIP)) {
            numFpointAddActionLogInsertQuery = insertLog
                    .replace("{{NUMBEFORE}}", String.valueOf(cf.getNumCurrentFreeShip()));
            updateFpointCustomerQuery = FREESHIP_CUSTOMER_UPDATE
                    .replace("{{NUMFREESHIP}}", String.valueOf(nextToBeReward))
                    .replace("{{CUSTOMER_ID}}", String.valueOf(cf.getCustomerId()));
        } else if (promotionType.equals(Variables.TYPE_FPOINT)) {
            numFpointAddActionLogInsertQuery = insertLog
                    .replace("{{NUMBEFORE}}", String.valueOf(cf.getNumCurrentFpoint()));
            updateFpointCustomerQuery = FPOINT_CUSTOMER_UPDATE
                    .replace("{{NUMFPOINT}}", String.valueOf(nextToBeReward))
                    .replace("{{CUSTOMER_ID}}", String.valueOf(cf.getCustomerId()));
        }

        try {
            dao.updateFpointReward(numFpointAddActionLogInsertQuery, updateFpointCustomerQuery);
        } catch (Exception e) {
            try {
                if (!InetAddress.getLocalHost().getHostName().contains("phongnhh-pc")) {
                    String exception = Utils.getErrorMsgFromStackTrace(e, "<br/>");
                    Utils.sendEmail(exception, ESCALATION_EMAIL_LIST,
                            "Exception when update reward for order id: " + cf.getOrderId());
                }
            } catch (UnknownHostException ex) {
                Logger.getLogger(RewardFpointProcess.class.getName()).log(Level.SEVERE, null, ex);
            }

            logger.log(Level.SEVERE, "**Exception when update fpoint for order id: {0}, "
                    + "email: {1}, fpointValue: {2}, fpoint before: {3}, "
                    + "fpoint after: {4}", new Object[]{cf.getOrderId(), cf.getCustomerEmail(),
                        valueRewarded, valueRewarded, nextToBeReward});
            logger.log(Level.SEVERE, "", e);
        }
    }

    public void applyGetCheapestItemXpercent(CustomerFpointInfo cf) {
        Double minPrice = cf.getMinPrice();
        Double actionValue = cf.getActionValue();
        Double maxRewardedValue = cf.getMaxRewardedValue();

        Double numFpointRewarded = minPrice * actionValue / 100;
        if (numFpointRewarded > maxRewardedValue) {
            numFpointRewarded = maxRewardedValue;
        }
        double nextToBeRewardFpoint = cf.getNumCurrentFpoint() + numFpointRewarded;

        String logMessage = "Reward fpoint amount " + numFpointRewarded + " for order: " + cf.getOrderId() + ", with number items of order is: " + cf.getNumItemsInOrder()
                + ", with grandTotal of order is: " + cf.getGrandTotal() + ", with min price of product: " + minPrice
                + ". This is for campaign rule id: " + cf.getCampaignId() + " and rule id is: " + cf.getRuleId();
        insertLogAndUpdateCE(cf, numFpointRewarded, nextToBeRewardFpoint, logMessage, Variables.TYPE_FPOINT);
    }

    public void applyGetHighestItemXpercent(CustomerFpointInfo cf) {
        Double maxPrice = cf.getMaxPrice();
        Double actionValue = cf.getActionValue();
        Double maxRewardedValue = cf.getMaxRewardedValue();
        Double numFpointRewarded = maxPrice * actionValue / 100;
        if (numFpointRewarded > maxRewardedValue) {
            numFpointRewarded = maxRewardedValue;
        }
        double nextToBeRewardFpoint = cf.getNumCurrentFpoint() + numFpointRewarded;

        String logMessage = "Reward fpoint amount " + numFpointRewarded + " for order: " + cf.getOrderId() + ", with number items of order is: " + cf.getNumItemsInOrder()
                + ", with grandTotal of order is: " + cf.getGrandTotal() + ", with max price of product: " + cf.getMaxPrice()
                + ". This is for campaign rule id: " + cf.getCampaignId() + " and rule id is: " + cf.getRuleId();
        insertLogAndUpdateCE(cf, numFpointRewarded, nextToBeRewardFpoint, logMessage, Variables.TYPE_FPOINT);
    }

    public void applyPromotionFpoint(CustomerFpointInfo cf) {
        Double nextToBeRewardFpoint = 0.0;
        if (cf.getActionType().equals(ACTION_ADD)) {
            nextToBeRewardFpoint = cf.getNumCurrentFpoint() + cf.getActionValue();
        } else if (cf.getActionType().equals(ACTION_MUL)) {
            nextToBeRewardFpoint = cf.getNumCurrentFpoint() + (cf.getTotalPoint() * (cf.getActionValue() - 1));
        } else {
            //default is multiply
            nextToBeRewardFpoint = cf.getNumCurrentFpoint() + (cf.getTotalPoint() * (cf.getActionValue() - 1));
        }
        Double fpointAmount = nextToBeRewardFpoint - cf.getNumCurrentFpoint();

        String logMessageFp = null;

        logMessageFp = "Reward fpoint amount " + fpointAmount + " for order: " + cf.getOrderId() + ", with number items of order is: " + cf.getNumItemsInOrder()
                + ", with grandTotal of order is: " + cf.getGrandTotal() + ". This is for campaign rule id: " + cf.getCampaignId() + " and rule id is: " + cf.getRuleId();
        insertLogAndUpdateCE(cf, fpointAmount, nextToBeRewardFpoint, logMessageFp, Variables.TYPE_FPOINT);
    }

    public void applyPromotionFreeship(CustomerFpointInfo cf) {
        Double nextToBeRewardFreeship = 0.0;
        if (cf.getActionType().equals(ACTION_ADD)) {
            nextToBeRewardFreeship = cf.getNumCurrentFreeShip() + cf.getActionValue();
        } else {
            //No action for multiply freeship. Rule should not happen
            //nextToBeRewardFreeship = numCurrentFreeShip * numPromotionToBeReward;
        }
        String logMessageFp = "Reward freeship amount " + cf.getActionValue() + " for order: " + cf.getOrderId() + ", with number items of order is: " + cf.getNumItemsInOrder()
                + ", with grandTotal of order is: " + cf.getGrandTotal() + ". This is for campaign rule id: " + cf.getCampaignId() + " and rule id is: " + cf.getRuleId();
        insertLogAndUpdateCE(cf, cf.getActionValue(), nextToBeRewardFreeship, logMessageFp, Variables.TYPE_FREESHIP);
    }

    public void rewardFpointProcess() {
        //Build FVIP Level Rule Map
        List<Object> r = dao.runNativeQuery(FVIP_LEVEL_RULE_SQL);
        for (Object o : r) {
            Object[] data = (Object[]) o;
            Integer fVipId = (Integer) data[0];
            fVipMap.put(fVipId, o);
        }
        //Get current reward fpoint rule
        List<Object> rwMainList = dao.runNativeQuery(CURRENT_FPOINT_REWARD_RULE);
        Integer currentRuleID = 0;
        Date currentFromDate = null;
        Date currentToDate = null;
        Integer currentCampaignId = 0;
        Double currentMaxRewardedValue = 0D;
        String currentActionName = null;
        String currentActionType = null;
        Double currentActionValue = 0D;
        String headerQueryGetOrderInfo = "select\n"
                + "  o.customer_id,\n"
                + "  o.customer_email                                                      as customerEmail,\n"
                + "  so.order_id                                                           as orderId,\n"
                + "  sum(IF(sb.bundle_id IS NULL, soi.qty, sb.qty * soi.qty))              as qty,\n"
                + "  min(if(sb.bundle_id is null, soi.price, (1 - sb.saving) * soi.price)) as minPriceItem,\n"
                + "  max(if(sb.bundle_id is null, soi.price, (1 - sb.saving) * soi.price)) as maxPriceItem,\n"
                + "  o.grand_total,\n"
                + "  pe.category_main_id,\n"
                + "  pe.category_mid_id,\n"
                + "  pe.sku\n"
                + "from fahasa_suborder so\n"
                + "  join fahasa_suborder_item soi on so.order_id = soi.order_id and so.suborder_id = soi.suborder_id\n"
                + "  LEFT JOIN fhs_catalog_product_entity pe ON soi.product_id = pe.entity_id\n"
                + "  LEFT JOIN fahasa_suborder_bundle sb\n"
                + "    on soi.bundle_id = sb.bundle_id AND soi.suborder_id = sb.suborder_id AND soi.bundle_type = sb.bundle_type\n"
                + "  JOIN fhs_sales_flat_order o on so.order_id = o.increment_id\n"
                + "WHERE o.status = 'complete'\n"
                + "  and so.status = 'complete'\n"
                + "  and so.parent_id is null\n"
                + "  and convert_tz(o.created_at, '+0:00', '+7:00') between '{{FROM_DATE}}' and '{{TO_DATE}}'\n"
                + "  and soi.price > 0\n"
                + "  and o.customer_id is not null\n"
                + "  and o.grand_total > 0\n"
                + "  and pe.sku not in ('1903041244305a','1903041244312a','codedientu100000','phieuquatang60000')\n"
                + "  and pe.category_mid_id not in (4388,5421,6152)\n";

        String footerQueryGetOrderInfo = " group by so.order_id";
        String queryGetOrderInfo = "";

        int i = 0;
        if (rwMainList != null) {
            for (Object d : rwMainList) {
                i++;
                Object[] dataTemp = (Object[]) d;
                String actionName = (String) dataTemp[0];
                String actionType = (String) dataTemp[2];
                Double actionValue = ((Integer) dataTemp[3]).doubleValue();
                Double maxRewardedValue = ((Integer) (dataTemp[4] == null ? MAX_VALUE : dataTemp[4])).doubleValue();
                Integer ruleId = (Integer) dataTemp[5];
                Integer campaignId = (Integer) dataTemp[6];
                Date fromDate = (Date) dataTemp[7];
                Date toDate = (Date) dataTemp[8];
                String typeFilter = (String) dataTemp[9];
                String valueFilter = (String) dataTemp[10];
                String actionFilter = (String) dataTemp[11];
                CustomerFpointInfo cf = new CustomerFpointInfo();
                
                if ((!currentRuleID.equals(ruleId)) || (i == rwMainList.size())) {
                    if (!queryGetOrderInfo.isEmpty()) {
                        queryGetOrderInfo += footerQueryGetOrderInfo;
                        cf.setRuleId(currentRuleID);
                        cf.setMaxRewardedValue(currentMaxRewardedValue);
                        cf.setActionName(currentActionName);
                        cf.setActionValue(currentActionValue);
                        cf.setActionType(currentActionType);
                        cf.setFromDate(currentFromDate);
                        cf.setToDate(currentToDate);
                        cf.setCampaignId(currentCampaignId);
                        processMainFpoint(queryGetOrderInfo, cf);
                    }
                    queryGetOrderInfo = headerQueryGetOrderInfo;
                    currentRuleID = ruleId;
                    currentFromDate = fromDate;
                    currentToDate = toDate;
                    currentCampaignId = campaignId;
                    currentActionName = actionName;
                    currentActionType = actionType;
                    currentMaxRewardedValue = maxRewardedValue;
                    currentActionValue = actionValue;
                    if (valueFilter != null) {
                        String includeExcludeString = actionFilter.equals(ACTION_INCLUDE) ? "in" : "not in";
                        queryGetOrderInfo += " and pe." + typeFilter + " " + includeExcludeString + " (" + valueFilter + ")\n";
                    }

                } else {
                    if (valueFilter != null) {
                        String includeExcludeString = actionFilter.equals("include") ? "in" : "not in";
                        queryGetOrderInfo += " and pe." + typeFilter + " " + includeExcludeString + " (" + valueFilter + ")\n";
                    }
                }
            }
        }
    }

    public void processMainFpoint(String query, CustomerFpointInfo ce) {
        //    Process main fpoint reward for orderId complete
        String tempSql = query
                .replace("{{FROM_DATE}}", DATE_FORMAT.format(ce.getFromDate()))
                .replace("{{TO_DATE}}", DATE_FORMAT.format(ce.getToDate()));
        List<Object> orderList = dao.runNativeQuery(tempSql);
        for (Object x : orderList) {
            CustomerFpointInfo cf = new CustomerFpointInfo();

            Object[] orderTemp = (Object[]) x;
            String customerId = String.valueOf((Long) orderTemp[0]);
            String customerEmail = (String) orderTemp[1];
            String orderId = (String) orderTemp[2];
            Long numItem = ((BigDecimal) orderTemp[3]).longValue();
            Double minPrice = ((BigDecimal) orderTemp[4]).doubleValue();
            Double maxPrice = ((BigDecimal) orderTemp[5]).doubleValue();
            Double grandTotal = ((BigDecimal) orderTemp[6]).doubleValue();

            // Get customer entity
            List<Object> tempData = dao.runNativeQuery(GET_CUSTOMER_ENTITY
                    .replace("{{CUSTOMER_ID}}", String.valueOf(customerId)));

            if (tempData.size() != 1) {
                LOGGER.log(Level.WARNING, "Email: {0} , Id: {1} - return not 1 customer entity",
                        new Object[]{customerEmail, customerId});
                Utils.sendEmail("Email " + customerEmail + " , with id " + customerId
                        + " - return not 1 customer entity", ESCALATION_EMAIL_LIST,
                        "Error. Email should return unique results for FPoint find customer entity");
                continue;
            }

            Object[] customerData = (Object[]) tempData.get(0);
            Double numCurrentFpoint = ((BigDecimal) customerData[0]).doubleValue();
            Integer currentFVipLevel = (Integer) customerData[1];
            Integer numCurrentFreeShip = (Integer) customerData[2];

            Object[] fVipData = (Object[]) fVipMap.get(currentFVipLevel);
            Double fPointPercent = ((BigDecimal) fVipData[2]).doubleValue();
            //numFPointEndForCurrentLevel could be null as last level have no fpoint end level
            int totalFPoint = (int) Math.floor(grandTotal * fPointPercent / 100);

            cf.setMinPrice(minPrice);

            cf.setNumCurrentFpoint(numCurrentFpoint);
            cf.setOrderId(orderId);
            cf.setCustomerEmail(customerEmail);
            cf.setCustomerId(customerId);
            cf.setMaxPrice(maxPrice);
            cf.setTotalPoint(totalFPoint);
            cf.setGrandTotal(grandTotal);
            cf.setNumCurrentFreeShip(numCurrentFreeShip);
            cf.setNumItemsInOrder(numItem);
            cf.setActionValue(ce.getActionValue());
            cf.setMaxRewardedValue(ce.getMaxRewardedValue());
            cf.setCampaignId(ce.getCampaignId());
            cf.setActionName(ce.getActionName());
            cf.setRuleId(ce.getRuleId());
            cf.setActionType(ce.getActionType());

            // Check exist order fpoint rewarded
            long numberProcess = (Long) dao.getResultAsObject(CHECK_EXIST_ORDER_REWARDED.replace("{{ORDERID}}", orderId));
            if (numberProcess > 0) {
                System.out.println("\nOrder Id: " + orderId + " bo qua vi da duoc xu ly\n");
                continue;
            }

            if (matchPromotionRule(cf) == true) {
                //Match promotion rule. Process with action for this promotion
                applyActionForThisOrder(cf);
            }
        }
    }

    public void applyActionForThisOrder(CustomerFpointInfo cf) {
        String actionName = cf.getActionName();
        if (actionName.equals(Action.CHEAPEST_ITEM_GET_X_PERCENT)) {
            applyGetCheapestItemXpercent(cf);
        } else if (actionName.equals(Action.HIGHEST_ITEM_GET_X_PERCENT)) {
            applyGetHighestItemXpercent(cf);
        } else if (actionName.equals(Action.ACTION_PROMOTION_FPOINT)) {
            applyPromotionFpoint(cf);
        } else if (actionName.equals(Action.ACTION_PROMOTION_FREESHIP)) {
            applyPromotionFreeship(cf);
        }
    }

    /**
     * Each rule will have a rule filter. This method here will go to each
     * filter to see if a current order info (hold inside CustomerFpointInfo
     * obj) will match a rule or not NOTE: all filter are AND. OR is currently
     * not handled
     *
     * @param cf
     * @param currentRuleId
     * @return
     */
    public boolean matchPromotionRule(CustomerFpointInfo cf) {
        boolean isMatchRule = true;
        Double grandTotal = cf.getGrandTotal();
        Long numItem = cf.getNumItemsInOrder();
        List<Object> filterList = dao.runNativeQuery(GET_RULE_FILTER.replace("{{RULE_ID}}", String.valueOf(cf.getRuleId())));
        for (Object d : filterList) {
            Object[] filterTemp = (Object[]) d;
            String cartType = (String) filterTemp[0];
            Double minActionValue = ((Integer) (filterTemp[2] == null ? -0 : filterTemp[2])).doubleValue();
            Double maxActionValue = ((Integer) (filterTemp[3] == null ? MAX_VALUE : filterTemp[3])).doubleValue();
            String value = (String) filterTemp[4];
            cf.setCartType(cartType);
            boolean ruleMatch = false;
            if (cartType.equals(Variables.TYPE_REWARD_GRAND_TOTAL)) {
                if (grandTotal >= minActionValue && grandTotal <= maxActionValue) {
                    ruleMatch = true;
                } else {
                    ruleMatch = false;
                }
            }
            if (cartType.equals(Variables.TYPE_REWARD_ITEM)) {
                if (numItem >= minActionValue && numItem <= maxActionValue) {
                    ruleMatch = true;
                } else {
                    ruleMatch = false;
                }
            }

            if (cartType.equals(Variables.TYPE_FILTER_CATEGORY_MAIN_ID) && value != null) {
                //Already handled in query 
                continue;
            }
            if (cartType.equals(Variables.TYPE_FILTER_CATEGORY_MID_ID) && value != null) {
                //Already handled in query 
                continue;
            }
            if (cartType.equals(Variables.TYPE_FILTER_SKU) && value != null) {
                //Already handled in query 
                continue;
            }
            isMatchRule &= ruleMatch;
        }
        return isMatchRule;
    }
}
