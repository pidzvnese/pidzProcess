/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.fahasa.fpointprocess;

import com.fahasa.com.fpointprocess.dao.DAO;
import com.fahasa.com.fpointprocess.utils.Utils;
import static com.fahasa.fpointprocess.FPointProcess.ESCALATION_EMAIL_LIST;
import com.fahasa.fpointprocess.dto.CustomerFpointInfo;
import com.fahasa.fpointprocess.model.Action;
import com.fahasa.fpointprocess.model.Variables;
import java.math.BigDecimal;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * When activate new account. First 3 order complete will receive FPoint 1st
 * order: receive 5K FPoint 2nd order: receive 10K FPoint 3rd order: receive 15K
 * FPoint
 *
 * @author phongnh92
 */
public class NumberOrderRewardFpointProcess {

    private static DAO dao;

    public static final String FIRST_ORDER = "1st";
    public static final String SECOND_ORDER = "2nd";
    public static final String THIRD_ORDER = "3rd";

    private static final Logger logger = Logger.getLogger(FPointProcess.class.getName());

    public static String NUMBER_ORDER_SQL = "select\n"
            + "	o.customer_id,\n"
            + "	o.customer_email,\n"
            + "	o.increment_id\n"
            + "from fhs_sales_flat_order o\n"
            + "	join fahasa_suborder so on so.order_id = o.increment_id\n"
            + "	join fhs_customer_entity ce on o.customer_id = ce.entity_id\n"
            + "where customer_id is not null\n"
            + "	and so.status = 'complete'\n"
            + "	and o.status = 'complete'\n"
            + "	and o.grand_total > 0\n"
            + "	and convert_tz(ce.created_at, '+0:00', '+7:00') >= '2018-10-22 00:00:01'\n"
            + "	and convert_tz(o.created_at, '+0:00', '+7:00') >= DATE_SUB(NOW(), INTERVAL 60 DAY)\n"
            + "	and ce.finish_warm_up_period = 0\n"
            + "order by o.increment_id";

    public static String ADD_ACTION_LOG_QUERY = "insert into fhs_purchase_action_log (account, customer_id, \n"
            + "action, number_order_rule, value,  amountAfter, updateBy, lastUpdated, order_id, suborder_id, description, \n"
            + "amountBefore, type, indexOrder) values ('{{EMAIL}}', {{CUSTOMER_ID}}, '{{ACTION}}', {{NUM_ORDER_RULE}}, {{NUM}}, {{NUMAFTER}}, \n"
            + "'tp script', now(), {{ORDER_ID}},'','{{DESC}}', \n"
            + "{{NUMBEFORE}}, '{{REWARDTYPE}}', {{INDEX}})";

    public static String CHECK_EXIST_ORDER_REWARDED = "select count(1)\n"
            + "from fhs_purchase_action_log\n"
            + "where customer_id = {{CUSTOMER_ID}} and order_id = {{ORDER_ID}} and number_order_rule > 0";

    public static String CHECK_INDEX_ORDER_REWARDED = "select COALESCE(max(indexOrder), 0)\n"
            + "from fhs_purchase_action_log\n"
            + "where customer_id = {{CUSTOMER_ID}}";

    public static String GET_CUSTOMER_ENTITY = "select fpoint, fpoint_accure_year, num_freeship\n"
            + "from fhs_customer_entity\n"
            + "where entity_id={{CUSTOMER_ID}}";

    public static String FPOINT_CUSTOMER_UPDATE = "update fhs_customer_entity set fpoint={{NUMFPOINT}} \n"
            + "where entity_id={{CUSTOMER_ID}}";

    public static String FREESHIP_CUSTOMER_UPDATE = "update fhs_customer_entity set num_freeship={{NUMFREESHIP}} \n"
            + "where entity_id={{CUSTOMER_ID}}";

    public static String GET_ORDER_REWARD_RULE = "select\n"
            + " id as ruleId,\n"
            + "	action_name,\n"
            + "	type,\n"
            + "	number_Order,\n"
            + "	value\n"
            + "from fhs_fpoint_number_order_rule\n"
            + "where end_status = 0";

    public static String UPDATE_FINISH_WARM_UP_PERIOD = "update fhs_customer_entity set finish_warm_up_period = 1\n"
            + "where entity_id = {{CUSTOMER_ID}}";

    public static void main(String[] args) {
        dao = new DAO();
        NumberOrderRewardFpointProcess rewardOrder = new NumberOrderRewardFpointProcess();
        rewardOrder.numberOrderRwProcess();
    }

    public void numberOrderRwProcess() {
        List<Object> orderList = dao.runNativeQuery(NUMBER_ORDER_SQL);
        for (Object x : orderList) {
            CustomerFpointInfo cf = new CustomerFpointInfo();
            Object[] orderTemp = (Object[]) x;
            String customerId = String.valueOf((Long) orderTemp[0]);
            String customerEmail = (String) orderTemp[1];
            String orderId = (String) orderTemp[2];

            // Get customer entity
            List<Object> tempData = dao.runNativeQuery(GET_CUSTOMER_ENTITY
                    .replace("{{CUSTOMER_ID}}", String.valueOf(customerId)));

            Object[] customerData = (Object[]) tempData.get(0);
            Double numCurrentFpoint = ((BigDecimal) customerData[0]).doubleValue();
//            Double fpointAccureYear = ((Integer) customerData[1]).doubleValue();
            Double numCurrentFreeship = ((Integer) customerData[2]).doubleValue();

            cf.setCustomerEmail(customerEmail);
            cf.setCustomerId(customerId);
            cf.setOrderId(orderId);

            // Check exist order id fpoint rewarded
            long numberProcess = (Long) dao.getResultAsObject(CHECK_EXIST_ORDER_REWARDED.replace("{{ORDER_ID}}", orderId)
                    .replace("{{CUSTOMER_ID}}", customerId));
            //If this account has not prior process, the fpoint_accure_year must be 0 (tai khoan moi)
//            if (numberProcess == 0 && fpointAccureYear != 0) {
//                System.out.println("Customer Id: " + customerId + " khong duoc xu ly voi FPoint accure year is: " + fpointAccureYear);
//                continue;
//            }
            if (numberProcess > 0) {
                System.out.println("\nCustomer Id: " + customerId + " with order Id: " + orderId + " bo qua vi da duoc xu ly\n");
                continue;
            }
            long index = (Long) dao.runNativeQueryGetSingleResult(CHECK_INDEX_ORDER_REWARDED
                    .replace("{{CUSTOMER_ID}}", String.valueOf(customerId)));
            if (index > 2) {
                //Done warm up process
                dao.executeUpdateNativeQuery(UPDATE_FINISH_WARM_UP_PERIOD.replace("{{CUSTOMER_ID}}", customerId));
                continue;
            }
            List<Object> rwMainList = dao.runNativeQuery(GET_ORDER_REWARD_RULE);
            for (Object a : rwMainList) {
                Object[] ruleTemp = (Object[]) a;
                Integer ruleId = (Integer) ruleTemp[0];
                String type = (String) ruleTemp[2];
                Integer numOrderRule = (Integer) ruleTemp[3];
                Integer ruleValue = (Integer) ruleTemp[4];

                cf.setRuleId(ruleId);
                if (type.contains(Variables.TYPE_FREESHIP)) {
                    if (index == 0 && numOrderRule == 1) {
                        getRewardForOrder(cf, numCurrentFreeship, ruleValue, FIRST_ORDER, type, 1);
                    } else if (index == 1 && numOrderRule == 2) {
                        getRewardForOrder(cf, numCurrentFreeship, ruleValue, SECOND_ORDER, type, 2);
                    } else if (index == 2 && numOrderRule == 3) {
                        getRewardForOrder(cf, numCurrentFreeship, ruleValue, THIRD_ORDER, type, 3);
                    } else {
                        System.out.println("bo di");
                    }
                } else if (type.contains(Variables.TYPE_FPOINT)) {
                    if (index == 0 && numOrderRule == 1) {
                        getRewardForOrder(cf, numCurrentFpoint, ruleValue, FIRST_ORDER, type, 1);
                    } else if (index == 1 && numOrderRule == 2) {
                        getRewardForOrder(cf, numCurrentFpoint, ruleValue, SECOND_ORDER, type, 2);
                    } else if (index == 2 && numOrderRule == 3) {
                        getRewardForOrder(cf, numCurrentFpoint, ruleValue, THIRD_ORDER, type, 3);
                    } else {
                        System.out.println("bo di");
                    }
                }
            }
        }
    }

    public void insertLogAndUpdateCE(CustomerFpointInfo cf, double numCurrentValue, double valueRewarded, double nextToBeReward, String logMessage, String promotionType, int index) {
        dao = new DAO();
        String updateFpointCustomerQuery = null;
        String updateFreeshipCustomerQuery = null;
        Integer ruleId = cf.getRuleId();
        String orderId = cf.getOrderId();
        String customerId = cf.getCustomerId();
        String customerEmail = cf.getCustomerEmail();

        String insertLog = ADD_ACTION_LOG_QUERY
                .replace("{{EMAIL}}", customerEmail)
                .replace("{{CUSTOMER_ID}}", customerId)
                .replace("{{ACTION}}", Action.X_ORDER_GET_Y_REWARD)
                .replace("{{NUM_ORDER_RULE}}", String.valueOf(ruleId))
                .replace("{{NUM}}", String.valueOf(valueRewarded))
                .replace("{{NUMAFTER}}", String.valueOf(nextToBeReward))
                .replace("{{ORDER_ID}}", orderId)
                .replace("{{DESC}}", logMessage)
                .replace("{{NUMBEFORE}}", String.valueOf(numCurrentValue))
                .replace("{{REWARDTYPE}}", promotionType)
                .replace("{{INDEX}}", String.valueOf(index));

        if (promotionType.equals(Variables.TYPE_FPOINT)) {
            updateFpointCustomerQuery = FPOINT_CUSTOMER_UPDATE
                    .replace("{{NUMFPOINT}}", String.valueOf(nextToBeReward))
                    .replace("{{CUSTOMER_ID}}", customerId);
            try {
                dao.updatePromotion(insertLog, updateFpointCustomerQuery);
            } catch (Exception e) {
                String exception = Utils.getErrorMsgFromStackTrace(e, "<br/>");
                Utils.sendEmail(exception, ESCALATION_EMAIL_LIST,
                        "Exception when update FPOINT for order id: " + orderId);
                logger.log(Level.SEVERE, "**Exception when update fpoint for order id: {0}, "
                        + "email: {1}, fpointValue: {2}, fpoint before: {3}, "
                        + "fpoint after: {4}", new Object[]{orderId, customerEmail,
                            valueRewarded, numCurrentValue, nextToBeReward});
                logger.log(Level.SEVERE, "", e);
            }
        } else if (promotionType.equals(Variables.TYPE_FREESHIP)) {
            updateFreeshipCustomerQuery = FREESHIP_CUSTOMER_UPDATE
                    .replace("{{NUMFREESHIP}}", String.valueOf(nextToBeReward))
                    .replace("{{CUSTOMER_ID}}", customerId);
            try {
                dao.updatePromotion(insertLog, updateFreeshipCustomerQuery);
            } catch (Exception e) {
                String exception = Utils.getErrorMsgFromStackTrace(e, "<br/>");
                Utils.sendEmail(exception, ESCALATION_EMAIL_LIST,
                        "Exception when update FREESHIP for order id: " + orderId);
                logger.log(Level.SEVERE, "**Exception when update fpoint for order id: {0}, "
                        + "email: {1}, freeship value: {2}, freeship before: {3}, "
                        + "freeship after: {4}", new Object[]{orderId, customerEmail,
                            valueRewarded, numCurrentValue, nextToBeReward});
                logger.log(Level.SEVERE, "", e);
            }
        }
    }

    public void getRewardForOrder(CustomerFpointInfo cf, Double numCurrentValue, Integer rewardValue, String numberOrder, String type, int index) {
        String customerID = cf.getCustomerId();
        String orderId = cf.getOrderId();
        Double nextToBeRewarded = numCurrentValue + rewardValue;

        String logMessage = "Reward " + type + " amount " + rewardValue + " for customerId: " + customerID + ", with " + numberOrder + " order! Order Id: " + orderId;
        insertLogAndUpdateCE(cf, numCurrentValue, rewardValue, nextToBeRewarded, logMessage, type, index);
    }
}
