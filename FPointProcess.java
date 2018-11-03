package com.fahasa.fpointprocess;

import com.fahasa.com.fpointprocess.dao.DAO;
import com.fahasa.com.fpointprocess.utils.Utils;
import com.fahasa.fpointprocess.dto.CustomerFpointInfo;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * + VIP will not get F-Point, but only incentive from upgrade level + VIP
 * student can get F-Point + Each suborder will receive F-Point once it status
 * is complete, and that amount is saved to each sub-order + vipLevel is saved
 * inside customer_entity with level = 0 is default and equivalent to silver
 * status (Customer need to be confirmed)
 *
 * @author Thang Pham
 */
public class FPointProcess {

    public static String SQL_QUERY = "select \n"
            + "so.order_id, so.suborder_id, so.f_point_amount,\n"
            + "sum(if(sb.bundle_id is null, soi.price, (1-sb.saving) * soi.price) * if(sb.bundle_id is null, soi.qty, sb.qty * soi.qty)) + ifnull(so.cod_fee, 0) + ifnull(so.shipping_fee, 0) - ifnull(so.discount_amount, 0) + ifnull(so.giftwrap_fee, 0) - ifnull(so.tryout_discount, 0) as item_total,\n"
            + "ce.email, ce.entity_id as customer_id, fo.created_at as order_created_at \n"
            + "from fahasa_suborder so \n"
            + "join fhs_sales_flat_order fo on so.order_id = fo.increment_id\n"
            + "join fhs_customer_entity ce on fo.customer_id = ce.entity_id\n"
            + "left join fhs_customer_entity_varchar not_confirm on ce.entity_id = not_confirm.entity_id and not_confirm.attribute_id=16\n"
            + "join fahasa_suborder_item soi on so.order_id = soi.order_id and so.suborder_id = soi.suborder_id \n"
            + "left join fahasa_suborder_bundle sb on soi.bundle_id = sb.bundle_id and soi.suborder_id = sb.suborder_id and soi.bundle_type = sb.bundle_type         \n"
            + "where so.parent_id is null and so.status = \"complete\"\n"
            + "and so.delivery_timestamp >= (CURDATE() - INTERVAL 30 DAY) and not_confirm.value is null\n"
            + "and so.f_point_amount = 0 \n"
            + "and soi.product_id not in (72984, 72985, 167338) \n"
            + "group by so.suborder_id";

    public static String FVIP_LEVEL_RULE_SQL = "select entity_id, label, percent_point_collect, \n"
            + "num_freeship_award, num_plastic_wrap_reward, num_point_begin_level, num_point_end_level, \n"
            + "value_reward_for_birthday from fhs_fvip_level_rule";

    public static String VIP_COMPANY_LEVEL_RULE_SQL = "select\n"
            + "  percent_point_collect\n"
            + "from fhs_vip_company_level_rule r\n"
            + "where r.company_id = '{{COMPANY_ID}}'\n"
            + "and entity_id = {{VIP_LEVEL}}";

    public static String FPOINT_SUBORDER_UPDATE = "update fahasa_suborder set f_point_amount={{FPOINT_VALUE}} \n"
            + "where suborder_id={{SUBORDER_ID}} and order_id='{{ORDER_ID}}'";

    public static String FPOINT_CUSTOMER_UDDATE = "update fhs_customer_entity set \n"
            + "fpoint={{FPOINT}}, vip_level={{VIP_LEVEL}}, num_freeship={{NUMFREESHIP}}, \n"
            + "fpoint_accure_year={{FPOINT_ACCURE_YEAR}}, num_rewarded_freeship={{NUM_REWARD_FREESHIP}} \n"
            + "where entity_id={{CUSTOMER_ID}}";

    public static String INSERT_FPOINT_ACTION_LOG = "insert into fhs_purchase_action_log (account, customer_id, \n"
            + "action, value, amountAfter, updateBy, lastUpdated, order_id, suborder_id, description, \n"
            + "amountBefore, type, company_code) values ('{{EMAIL}}', {{CUSTOMER_ID}}, 'accure fpoint', {{FPOINT}}, {{FPOINTAFTER}}, \n"
            + "'tp script', now(), '{{ORDERID}}', {{SUBORDERID}}, '{{DESC}}', \n"
            + "{{FPOINTBEFORE}}, 'fpoint', '{{COMPANY_CODE)}}');";

    public static String INSERT_NUM_FREESHIP_ACTION_LOG = "insert into fhs_purchase_action_log (account, \n"
            + "action, value, amountAfter, updateBy, lastUpdated, order_id, suborder_id, description, \n"
            + "amountBefore, type, company_code) values ('{{EMAIL}}', 'add more freeship', {{NUMFREESHIP}}, {{NUMFREESHIPAFTER}}, \n"
            + "'tp script', now(), '{{ORDERID}}', {{SUBORDERID}}, 'add more num freeship when hit fpoint milestone', \n"
            + "{{NUMFREESHIPBEFORE}}, 'freeship', {{COMPANY_CODE)}});";

    public static String GET_CUSTOMER_ENTITY = "select\n"
            + "  ce.fpoint,\n"
            + "  ce.vip_level,\n"
            + "  ce.num_freeship,\n"
            + "  ce.fpoint_accure_year,\n"
            + "  ce.num_rewarded_freeship,\n"
            + "  ev.value as companyId\n"
            + "from fhs_customer_entity ce\n"
            + "  join fhs_customer_entity_varchar ev on ev.entity_id = ce.entity_id and ev.attribute_id = 178\n"
            + "where ce.entity_id = '{{CUSTOMER_ID}}'";

    public static String GET_COMPANY_ID = "select companyId from fhs_vip_company";

    public static String INSERT_UPGRADE_LEVEL_ACTION_LOG = "insert into fhs_purchase_action_log (account, \n"
            + "action, amountAfter, updateBy, lastUpdated, order_id, suborder_id, description, \n"
            + "amountBefore, type, company_code) values ('{{EMAIL}}', 'fvip level upgrade', {{FVIPAFTER}}, \n"
            + "'tp script', now(), '{{ORDERID}}', {{SUBORDERID}}, 'fvip level upgrade', \n"
            + "{{FVIPBEFORE}}, 'fpoint',{{COMPANY_CODE)}});";

    /**
     * type: fpoint, freeship value: giá trị nhân lên. Ex: nếu là fpoint thì
     * value=3 nghĩa là nhân 3 fpoint. Nếu là freeship nghĩa là tặng 3 freeship
     * nếu thoả rule giỏ hàng status: 0 là on going. 1 là complete This process
     * only handle fpoint. Freeship will handle on a separate process, as
     * freeship will generally deal with the entire order, instead of suborder
     * like fpoint
     */
    public static int SILVER_LEVEL = 0;
    public static int GOLD_LEVEL = 1;
    public static int DIAMOND_LEVEL = 2;
    public static final int FREESHIP_STEP_SIZE = 10000; //Equal value of 1.000.000 vnd
    public static final Double MAX_VALUE = 10000000.0;
    //Mean max nhan diem khong the vuot qua so nay
    public static final Integer MAX_FPOINT_NHAN_DIEM = 5;
    public final static String[] ESCALATION_EMAIL_LIST = new String[]{
        "phamtn8@gmail.com",
        "trung.qn.1010@gmail.com"
    };

    private static DAO dao;
    private Map<Integer, Object> fVipMap = new HashMap<>();
    private static final Logger logger = Logger.getLogger(FPointProcess.class.getName());

    public static void main(String[] args) {
        dao = new DAO();
        FPointProcess fpoint = new FPointProcess();
        fpoint.mainContronller();
    }

    /**
     * Check if a current suborder total amount match a cart rule condition, if;
     * so return the promotion rule. If bound value is null, meaning infinite
     * Return null, if not match Only look at fpoint promotion, and assume, only
     * one rule can match a sub-order value
     */
    public void mainContronller() {
        //Build FVIP Level Rule Map
        List<Object> r = dao.runNativeQuery(FVIP_LEVEL_RULE_SQL);
        for (Object o : r) {
            Object[] data = (Object[]) o;
            Integer fVipId = (Integer) data[0];
            fVipMap.put(fVipId, o);
        }
        //Get current promotion fpoint/freeship rule
//        List<Object> prMainList = dao.runNativeQuery(CURRENT_FPOINT_PROMOTION_RULE);
        //Process main fpoint reward for sub-order complete
        String tempQuery = SQL_QUERY;
        List<Object> results = dao.runNativeQuery(tempQuery);
        logger.log(Level.INFO, "** Process {0} results **", results.size());
        List<Object> companyResults = dao.runNativeQuery(GET_COMPANY_ID);
        Set<String> companyIdSet = new HashSet<>();
        for (Object c : companyResults) {
            String companyId = (String) c;
            companyIdSet.add(companyId);
        }

        //Process per sub-order
        for (Object o : results) {
            Object[] data = (Object[]) o;
            String orderId = (String) data[0];
            Long subOrderId = (Long) data[1];
            Integer fPointAmount = (Integer) data[2];
            Double suborderAmount = Math.floor(((BigDecimal) data[3]).doubleValue());
            String customerEmail = (String) data[4];
            Long customerId = (Long) data[5];
            CustomerFpointInfo cf = new CustomerFpointInfo();
            cf.setOrderId(orderId);
            cf.setSubOrderId(subOrderId);
            cf.setSuborderAmount(suborderAmount);
            cf.setCustomerId(String.valueOf(customerId));
            cf.setCustomerEmail(customerEmail);
            cf.setSuborderAmount(suborderAmount);
//            Date orderCreatedAt = (Date) data[6];
//            String customerEmail = "phamtn8@gmail.com";
//            Long customerId = Long.valueOf(18129);
            //Obtain fresh new customer_entity data, as this could change by previous update fpoint
            List<Object> tempData = dao.runNativeQuery(GET_CUSTOMER_ENTITY
                    .replace("{{CUSTOMER_ID}}", String.valueOf(customerId)));
            if (tempData.size() != 1) {
                logger.log(Level.WARNING, "Email: {0} , Id: {1} - return not 1 customer entity",
                        new Object[]{customerEmail, customerId});
                try {
                    if (!InetAddress.getLocalHost().getHostName().contains("phongnhh-pc")) {
                        Utils.sendEmail("Email " + customerEmail + " , with id " + customerId
                                + " - return not 1 customer entity", ESCALATION_EMAIL_LIST,
                                "Error. Email should return unique results for FPoint find customer entity");
                    }
                } catch (UnknownHostException ex) {
                    Logger.getLogger(FPointProcess.class.getName()).log(Level.SEVERE, null, ex);
                }

                continue;
            }
            Object[] customerData = (Object[]) tempData.get(0);
            Integer customerCurrentFPoint = ((BigDecimal) customerData[0]).intValue();
            Integer currentFVipLevel = (Integer) customerData[1];
            Integer numCurrentFreeShip = (Integer) customerData[2];
            Integer currentFpoitnAccrYear = (Integer) customerData[3];
            Integer numRewardedFreeship = (Integer) customerData[4];
            String vipCompanyCode = (String) customerData[5];

            cf.setNumCurrentFpoint(customerCurrentFPoint.doubleValue());
            cf.setCurrentFVipLevel(currentFVipLevel);
            cf.setNumCurrentFreeShip(numCurrentFreeShip);
            cf.setCurrentFpointAccrYear(currentFpoitnAccrYear);
            cf.setNumRewardedFreeship(numRewardedFreeship);

            if (!companyIdSet.contains(vipCompanyCode)) {
                //Only process data when fPoint = 0, meaning that this suborder is not yet processed
                if (fPointAmount == 0 && suborderAmount > 0) {
                    cf.setVipCompanyCode("");
                    Object[] fVipData = (Object[]) fVipMap.get(currentFVipLevel);
                    Double fPointPercent = ((BigDecimal) fVipData[2]).doubleValue();
                    fpointMainProcess(cf, fPointPercent);
                }
            } else {
                if (fPointAmount == 0 && suborderAmount > 0) {
                    try {
                        cf.setVipCompanyCode(vipCompanyCode);
                        Double fPointPercent = ((BigDecimal) dao.runNativeQueryGetSingleResult(VIP_COMPANY_LEVEL_RULE_SQL
                                .replace("{{COMPANY_ID}}", vipCompanyCode)
                                .replace("{{VIP_LEVEL}}", String.valueOf(currentFVipLevel)))).doubleValue();
                        fpointMainProcess(cf, fPointPercent);
                    } catch (Exception e) {
                        cf.setVipCompanyCode("");
                        Object[] fVipData = (Object[]) fVipMap.get(currentFVipLevel);
                        Double fPointPercent = ((BigDecimal) fVipData[2]).doubleValue();
                        fpointMainProcess(cf, fPointPercent);
                    }

                }
            }
        }
    }

    public void fpointMainProcess(CustomerFpointInfo cf, double fPointPercent) {
        String orderId = cf.getOrderId();
        String customerEmail = cf.getCustomerEmail();
        String customerId = cf.getCustomerId();
        Double numCurrentFpoint = cf.getNumCurrentFpoint();
        Integer numCurrentFreeship = cf.getNumCurrentFreeShip();
        Integer numRewardedFreeship = cf.getNumRewardedFreeship();
        Integer currentFpointAccrYear = cf.getCurrentFpointAccrYear();
        Integer currentFvipLevel = cf.getCurrentFVipLevel();
        Double suborderAmount = cf.getSuborderAmount();
        String subOrderId = String.valueOf(cf.getSubOrderId());

        //numFPointEndForCurrentLevel could be null as last level have no fpoint end level
//        Integer numFPointEndForCurrentLevel = (Integer) fVipData[6];
        int subOrderFPoint = (int) Math.floor(suborderAmount * fPointPercent / 100);

        //update to subOrderFPoint to SubOrder
        String updateSubOrderFpointQuery = FPOINT_SUBORDER_UPDATE
                .replace("{{FPOINT_VALUE}}", String.valueOf(subOrderFPoint))
                .replace("{{SUBORDER_ID}}", subOrderId)
                .replace("{{ORDER_ID}}", orderId);
        //update customer fPoint
        Double fPointAfter = numCurrentFpoint + subOrderFPoint;
        Integer fPointAccureYearAfter = currentFpointAccrYear + subOrderFPoint;
        Integer nextToBeRewardFreeship = numCurrentFreeship;

        String numFreeshipAddActionLogInsertQuery = null;
        //Each 10000 Fpoint (1.000.000 vnd), we will reward customer 1 freeship
        int numFreeToBeReward = (int) Math.floor(fPointAccureYearAfter / FREESHIP_STEP_SIZE);
        if (numFreeToBeReward > 0) {
            //numRewardedFreeship is freshly query num_rewarded_freeship from db
            //so substract from it to see if we need to reward this current customer
            numFreeToBeReward = numFreeToBeReward - numRewardedFreeship;
            if (numFreeToBeReward > 0) {
                //Reward more Freeship, and update num_rewarded_freeship
                nextToBeRewardFreeship = numCurrentFreeship + numFreeToBeReward;
                String logMessage = "Add more num freeship per " + FREESHIP_STEP_SIZE
                        + " fpoints. FPoint accure year is: " + fPointAccureYearAfter
                        + " . Number of already rewarded freeship: " + numRewardedFreeship;
                //action log for reward freeship
                numFreeshipAddActionLogInsertQuery = "insert into fhs_purchase_action_log (account, customer_id, \n"
                        + "action, value, amountAfter, updateBy, lastUpdated, order_id, suborder_id, description, \n"
                        + "amountBefore, type, company_code) values ('{{EMAIL}}', {{CUSTOMER_ID}}, 'add more freeship', {{NUMFREESHIP}}, {{NUMFREESHIPAFTER}}, \n"
                        + "'tp script', now(), '{{ORDERID}}', {{SUBORDERID}}, '{{DESC}}', \n"
                        + "{{NUMFREESHIPBEFORE}}, 'freeship', '{{COMPANY_CODE)}}');";
                numFreeshipAddActionLogInsertQuery = numFreeshipAddActionLogInsertQuery
                        .replace("{{EMAIL}}", customerEmail)
                        .replace("{{CUSTOMER_ID}}", customerId)
                        .replace("{{NUMFREESHIP}}", String.valueOf(numFreeToBeReward))
                        .replace("{{NUMFREESHIPAFTER}}", String.valueOf(nextToBeRewardFreeship))
                        .replace("{{ORDERID}}", orderId)
                        .replace("{{SUBORDERID}}", subOrderId)
                        .replace("{{DESC}}", logMessage)
                        .replace("{{NUMFREESHIPBEFORE}}", String.valueOf(numCurrentFreeship))
                        .replace("{{COMPANY_CODE)}}", String.valueOf(cf.getVipCompanyCode()));
            }
        }
        String updateFPointCustomerQuery = FPOINT_CUSTOMER_UDDATE
                .replace("{{FPOINT}}", String.valueOf(fPointAfter))
                .replace("{{VIP_LEVEL}}", String.valueOf(currentFvipLevel))
                .replace("{{NUMFREESHIP}}", String.valueOf(nextToBeRewardFreeship))
                .replace("{{FPOINT_ACCURE_YEAR}}", String.valueOf(fPointAccureYearAfter))
                .replace("{{NUM_REWARD_FREESHIP}}",
                        String.valueOf(numFreeToBeReward + numRewardedFreeship))
                .replace("{{CUSTOMER_ID}}", customerId);

        //action log for add new fpoint
        String insertFPointDesc = null;
        if (cf.getVipCompanyCode().isEmpty() || cf.getVipCompanyCode() == null ) {
            insertFPointDesc = "Fpoint for success delivery order. " + " suborder amount: " + suborderAmount
                + " Current VIP level: " + currentFvipLevel;
        } else {
            insertFPointDesc = "Fpoint for success delivery order. " + " suborder amount: " + suborderAmount
                + " Current VIP level: " + currentFvipLevel + " company vip code is: " + cf.getVipCompanyCode();
        }
        
        String fPointActionLogInsert = INSERT_FPOINT_ACTION_LOG
                .replace("{{EMAIL}}", customerEmail)
                .replace("{{CUSTOMER_ID}}", customerId)
                .replace("{{FPOINT}}", String.valueOf(subOrderFPoint))
                .replace("{{FPOINTAFTER}}", String.valueOf(fPointAfter))
                .replace("{{ORDERID}}", cf.getOrderId())
                .replace("{{SUBORDERID}}", subOrderId)
                .replace("{{DESC}}", insertFPointDesc)
                .replace("{{FPOINTBEFORE}}", String.valueOf(numCurrentFpoint))
                .replace("{{COMPANY_CODE)}}", String.valueOf(cf.getVipCompanyCode()));
        try {
            dao.updateFPoint(updateSubOrderFpointQuery, updateFPointCustomerQuery,
                    fPointActionLogInsert, numFreeshipAddActionLogInsertQuery);
        } catch (Exception e) {
            try {
                if (!InetAddress.getLocalHost().getHostName().contains("phongnhh-pc")) {
                    String exception = Utils.getErrorMsgFromStackTrace(e, "<br/>");
                    Utils.sendEmail(exception, ESCALATION_EMAIL_LIST,
                            "Exception when update FPOINT for order id: " + orderId
                            + ", suborder id: " + subOrderId);
                }
            } catch (UnknownHostException ex) {
                Logger.getLogger(FPointProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
            logger.log(Level.SEVERE, "**Exception when update fpoint for order id: {0}, "
                    + "suborder id: {1}, email: {2}, fpointValue: {3}, fpoint before: {4}, "
                    + "fpoint after: {5}", new Object[]{orderId, subOrderId, customerEmail,
                        subOrderFPoint, numCurrentFpoint, fPointAfter});
            logger.log(Level.SEVERE, "", e);
        }
    }
}
