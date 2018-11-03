package com.fahasa.fpointprocess;

import com.fahasa.com.fpointprocess.dao.DAO;
import com.fahasa.com.fpointprocess.utils.Utils;
import static com.fahasa.fpointprocess.FPointProcess.ESCALATION_EMAIL_LIST;
import com.fahasa.fpointprocess.model.Action;
import com.fahasa.fpointprocess.model.Variables;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author phongnh92
 */
public class ReferProcess {

    public static String ORDER_SQL = "select\n"
            + "  o.increment_id,\n"
            + "  ce.entity_id                 as customerId,\n"
            + "  ce.email                     as customerEmail,\n"
            + "  o.grand_total as grandTotal,\n"
            + "  ce.fpoint,\n"
            + "  o.coupon_code,\n"
            + "  ce.refer_rule,\n"
            + "  r.action,\n"
            + "  r.value,\n"
            + "  o.created_at\n"
            + "from fhs_sales_flat_order o\n"
            + "  join fhs_customer_entity ce on o.coupon_code = ce.refer_code and ce.refer_status = 1\n"
            + "  join fhs_fpoint_refer_rule r on r.refer_rule_id = ce.refer_rule and r.status = 1\n"
            + "where o.coupon_code is not null\n"
            + "      and ce.refer_code is not null\n"
            + "      and ce.refer_rule is not null\n"
            + "      and o.created_at >= (CURDATE() - INTERVAL 45 DAY)\n"
            + "      and o.status = 'complete'\n"
            + "      and o.customer_id is not null\n"
            + "group by o.entity_id\n"
            + "order by ce.entity_id";

    public static String NUM_REFER_ADD_ACTION_LOG_QUERY = "insert into fhs_purchase_action_log (account, customer_id, referRuleId, action, value,\n"
            + "amountAfter, updateBy, lastUpdated,order_id, suborder_id, description,\n"
            + "amountBefore, type, refer_code) values ('{{EMAIL}}', '{{CUSTOMER_ID}}', '{{REFER_RULE_ID}}','{{ACTION}}',\n"
            + "'{{NUM}}', '{{NUMAFTER}}', 'tp script', now(), '{{ORDERID}}', '', '{{DESC}}',\n"
            + "'{{NUMBEFORE}}', '{{TYPE}}', '{{REFER_CODE}}')";

    public static String CHECK_EXIST_ORDER_REFER = "select count(1)\n"
            + "from fhs_purchase_action_log\n"
            + "where order_id = '{{ORDERID}}' and action = 'add refer fpoint'";

    public static String FPOINT_CUSTOMER_UPDATE = "update fhs_customer_entity set fpoint={{NUMFPOINT}} \n"
            + "where entity_id={{CUSTOMER_ID}}";
    
    private static final Logger logger = Logger.getLogger(FPointProcess.class.getName());

    private static DAO dao;

    public static void main(String[] args) {
        dao = new DAO();
        ReferProcess refer = new ReferProcess();
        refer.ReferProcessMainController();
    }

    public void ReferProcessMainController() {
        List<Object> ordersList = dao.runNativeQuery(ORDER_SQL);

        Double numCurrentFpoint = 0.0;
        Long oldCustomerId = -1L;

        for (Object x : ordersList) {
            Object[] orderTemp = (Object[]) x;
            String orderId = (String) orderTemp[0];
            Long customerId = (Long) orderTemp[1];
            String customerEmail = (String) orderTemp[2];
            Double grandTotal = ((BigDecimal) orderTemp[3]).doubleValue();
            String couponCode = (String) orderTemp[5];
            Integer referRuleId = (Integer) orderTemp[6];
            String action = (String) orderTemp[7];
            Integer numReferToBeReward = (Integer) orderTemp[8];
            //list is order by customer entity id, so the below check if we have move to a new customer
            //If it is new, get new FPoint value
            if (!customerId.equals(oldCustomerId)) {
                oldCustomerId = customerId;
                numCurrentFpoint = ((BigDecimal) orderTemp[4]).doubleValue();
            }

            long soLanDaXuLy = (Long) dao.getResultAsObject(CHECK_EXIST_ORDER_REFER
                    .replace("{{ORDERID}}", orderId));

            if (soLanDaXuLy > 0) {
                System.out.println("OrderID = " + orderId + " with refer code: " + couponCode + " bo qua, do da tung duoc xu ly");
                continue;
            }

            double beforeFpoint;
            beforeFpoint = numCurrentFpoint;
            if (action.equals("add")) {
                numCurrentFpoint = numCurrentFpoint + numReferToBeReward;
            } else {
                numCurrentFpoint = numCurrentFpoint + (grandTotal * numReferToBeReward / 100);
            }
            double value = numCurrentFpoint - beforeFpoint;
            String logMessageRp = "Reward refer fpoint amount " + numReferToBeReward
                    + " for order: " + orderId + " with Customer ID: " + customerId + ", with grand total: " + grandTotal
                    + " . This is for refer code: " + couponCode + " with Refer Rule Id is: " + referRuleId + "";

            String numReferAddActionLogInsertQuery = NUM_REFER_ADD_ACTION_LOG_QUERY
                    .replace("{{EMAIL}}", customerEmail)
                    .replace("{{CUSTOMER_ID}}", String.valueOf(customerId))
                    .replace("{{ACTION}}", Action.ACTION_REFER_FPOINT)
                    .replace("{{REFER_RULE_ID}}", String.valueOf(referRuleId))
                    .replace("{{NUM}}", String.valueOf(value))
                    .replace("{{NUMBEFORE}}", String.valueOf(beforeFpoint))
                    .replace("{{NUMAFTER}}", String.valueOf(numCurrentFpoint))
                    .replace("{{ORDERID}}", String.valueOf(orderId))
                    .replace("{{DESC}}", logMessageRp)
                    .replace("{{TYPE}}", Variables.TYPE_FPOINT)
                    .replace("{{REFER_CODE}}", couponCode);

            String updateFPointCustomerQuery = FPOINT_CUSTOMER_UPDATE
                    .replace("{{NUMFPOINT}}", String.valueOf(numCurrentFpoint))
                    .replace("{{CUSTOMER_ID}}", String.valueOf(customerId));

            try {
                dao.updateFpointRefer(numReferAddActionLogInsertQuery, updateFPointCustomerQuery);
            } catch (Exception e) {
                try {
                    if (!InetAddress.getLocalHost().getHostName().contains("phongnhh-pc")) {
                        String exception = Utils.getErrorMsgFromStackTrace(e, "<br/>");
                        if (!InetAddress.getLocalHost().getHostName().contains("phongnhh-pc")) {
                            Utils.sendEmail(exception, ESCALATION_EMAIL_LIST,
                                    "Exception when update refer reward FPOINT for order id: " + orderId);
                        }
                    }
                } catch (UnknownHostException ex) {
                    Logger.getLogger(FPointProcess.class.getName()).log(Level.SEVERE, null, ex);
                }
                logger.log(Level.SEVERE, "**Exception when update refer fpoint for order id: {0}, "
                        + "email: {1}, fpointValue: {2}, fpoint before: {3}, "
                        + "fpoint after: {4}", new Object[]{orderId, customerEmail,
                            value, numCurrentFpoint, numCurrentFpoint});
                logger.log(Level.SEVERE, "", e);
            }
        }
    }
}
