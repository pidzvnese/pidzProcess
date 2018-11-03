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
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author phongnh92
 */
public class ReviewFpointProcess {

    private static DAO dao;

    public static final Integer MAX_VALUE = 10000;

    public static String CURRENT_FPOINT_REVIEW_RULE = "select\n"
            + "  id as reviewCampaignId,\n"
            + "  action_name,\n"
            + "  review_type,\n"
            + "  type,\n"
            + "  action,\n"
            + "  value,\n"
            + "  min_value   as minValue,\n"
            + "  max_value   as `maxValue`,\n"
            + "  from_date,\n"
            + "  to_date\n"
            + "from fhs_fpoint_review_rule\n"
            + "where end_status = 0";
    // khong tinh so khoang trang trong review!
    public static String REVIEW_INFO = "select\n"
            + "  a.customer_id,\n"
            + "  a.email                                      as customerEmail,\n"
            + "  a.review_id,\n"
            + "  character_length(replace(replace(replace(a.detail, \" \", ''), \"\\n\", \"\"), \"\\r\", \"\")) as numberCharacter,\n"
            + "  sum(a.type = 'like')                         as numLike,\n"
            + "  a.entity_pk_value\n"
            + "from\n"
            + "  (select\n"
            + "     d.customer_id,\n"
            + "     ce.email,\n"
            + "     r.review_id,\n"
            + "     d.detail,\n"
            + "     ra.type,\n"
            + "     ra.customer_email,\n"
            + "     r.entity_pk_value\n"
            + "   from fhs_review r\n"
            + "     join fhs_review_detail d on d.review_id = r.review_id\n"
            + "     join fhs_customer_entity ce on d.customer_id = ce.entity_id\n"
            + "     left join fhs_reviews_action ra\n"
            + "       on ra.review_id = r.review_id and\n"
            + "          convert_tz(ra.created_at, '+0:00', '+7:00') between '{{FROM_DATE}}' and '{{TO_DATE}}'\n"
            + "   where r.status_id = 1\n"
            + "         and convert_tz(r.created_at, '+0:00', '+7:00') between '{{FROM_DATE}}' and '{{TO_DATE}}'\n"
            + "  ) a\n"
            + "group by a.review_id";

    public static String ADD_ACTION_LOG_QUERY = "insert into fhs_purchase_action_log (account, customer_id, \n"
            + "action, reviewCampaignId, value,  amountAfter, updateBy, lastUpdated, review_id, description, \n"
            + "amountBefore, type, product_id) values ('{{EMAIL}}', {{CUSTOMER_ID}}, '{{ACTION}}', {{REVIEW_CAMPAIGN_ID}} ,{{NUM}}, {{NUMAFTER}}, \n"
            + "'tp script', now(), {{REVIEW_ID}}, '{{DESC}}', \n"
            + "'{{NUMBEFORE}}', '{{REWARDTYPE}}', {{PRODUCT_ID}});";

    //There should be MAX 1 campaign for each review Id
    public static String CHECK_EXIST_REVIEW_REWARDED = "select count(*)\n"
            + "from fhs_purchase_action_log\n"
            + "where product_id = {{PRODUCT_ID}}\n"
            + "and reviewCampaignId = {{REVIEW_CAMPAIGN_ID}}\n"
            + "and customer_id = {{CUS_ID}}";

    public static String GET_CUSTOMER_ENTITY = "select fpoint\n"
            + " from fhs_customer_entity \n"
            + "where entity_id={{CUSTOMER_ID}}";

    public static String FPOINT_CUSTOMER_UPDATE = "update fhs_customer_entity set fpoint={{NUMFPOINT}} \n"
            + "where entity_id={{CUSTOMER_ID}}";

    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Logger logger = Logger.getLogger(FPointProcess.class.getName());

    public static void main(String[] args) {
        dao = new DAO();
        ReviewFpointProcess review = new ReviewFpointProcess();
        review.reviewFpointProcess();
    }

    public void addFPointForReview(CustomerFpointInfo cf) {
        double nextToBeRewardFpoint = cf.getNumCurrentFpoint() + cf.getActionValue();
        String logMessage = null;
        if (cf.getActionName().equals(Action.REVIEW_X_LIKE_GET_Y_FPOINT)) {
            logMessage = "Reward fpoint amount " + cf.getActionValue() + " for review: " + cf.getReviewId()
                    + ", with num like is: " + cf.getNumLike() + ". This is for review campaign rule id: " + cf.getReviewCampaignId();
        } else if (cf.getActionName().equals(Action.REVIEW_X_CHARACTER_GET_Y_FPOINT)) {
            logMessage = "Reward fpoint amount " + cf.getActionValue() + " for review: " + cf.getReviewId()
                    + ", with num character is: " + cf.getNumCharacter() + ". This is for campaign rule id: " + cf.getReviewCampaignId();
        }
        insertLogAndUpdateCE(cf, cf.getActionValue(), nextToBeRewardFpoint, logMessage, Variables.TYPE_FPOINT);
    }
    
    public void insertLogAndUpdateCE(CustomerFpointInfo cf, double valueRewarded, double nextToBeReward, String logMessage, String rewardType) {
        dao = new DAO();
        Double numCurentFpoint = cf.getNumCurrentFpoint();
        String productId = cf.getProductId();

        String numFpointAddActionLogInsertQuery = ADD_ACTION_LOG_QUERY
                .replace("{{EMAIL}}", cf.getCustomerEmail())
                .replace("{{CUSTOMER_ID}}", cf.getCustomerId())
                .replace("{{ACTION}}", cf.getActionName())
                .replace("{{REVIEW_CAMPAIGN_ID}}", String.valueOf(cf.getReviewCampaignId()))
                .replace("{{NUM}}", String.valueOf(valueRewarded))
                .replace("{{NUMAFTER}}", String.valueOf(nextToBeReward))
                .replace("{{REVIEW_ID}}", cf.getReviewId())
                .replace("{{DESC}}", logMessage)
                .replace("{{NUMBEFORE}}", String.valueOf(numCurentFpoint))
                .replace("{{REWARDTYPE}}", rewardType)
                .replace("{{PRODUCT_ID}}", productId);

        String updateFpointCustomerQuery = FPOINT_CUSTOMER_UPDATE
                .replace("{{NUMFPOINT}}", String.valueOf(nextToBeReward))
                .replace("{{CUSTOMER_ID}}", String.valueOf(cf.getCustomerId()));
        
        try {
            dao.updateFpointReward(numFpointAddActionLogInsertQuery, updateFpointCustomerQuery);
        } catch (Exception e) {
            try {
                if (!InetAddress.getLocalHost().getHostName().contains("phongnhh-pc")) {
                    String exception = Utils.getErrorMsgFromStackTrace(e, "<br/>");
                    if (!InetAddress.getLocalHost().getHostName().contains("phongnhh-pc")) {
                        Utils.sendEmail(exception, ESCALATION_EMAIL_LIST,
                                "Exception when update fpoint review for review id: " + cf.getReviewId());
                    }
                }
            } catch (UnknownHostException ex) {
                Logger.getLogger(FPointProcess.class.getName()).log(Level.SEVERE, null, ex);
            }
            logger.log(Level.SEVERE, "**Exception when update fpoint reward for review id: {0}, "
                    + "email: {1}, fpointValue: {2}, fpoint before: {3}, "
                    + "fpoint after: {4}", new Object[]{cf.getReviewId(), cf.getCustomerEmail(),
                        String.valueOf(valueRewarded), String.valueOf(numCurentFpoint), String.valueOf(nextToBeReward)});
            logger.log(Level.SEVERE, "", e);
        }
    }

    public void applyActionForThisReview(CustomerFpointInfo cf) {
        addFPointForReview(cf);
    }

    public boolean matchReviewRule(CustomerFpointInfo cf) {
        boolean isMatchRule = false;
        String reviewType = cf.getReviewType();
        Double minActionValue = cf.getMinRuleValue();
        Double maxActionValue = cf.getMaxRuleValue();
        Long numCharacter = cf.getNumCharacter();
        Long numLike = cf.getNumLike();

        if (reviewType.equals(Variables.TYPE_REVIEW_CHARACTER)) {
            if (numCharacter >= minActionValue && numCharacter <= maxActionValue) {
                isMatchRule = true;
            }
        } else if (reviewType.equals(Variables.TYPE_REVIEW_LIKE)) {
            if (numLike >= minActionValue && numLike <= maxActionValue) {
                isMatchRule = true;
            }
        }
        return isMatchRule;
    }

    public void reviewFpointProcess() {
        //Get current review rule
        List<Object> rwMainList = dao.runNativeQuery(CURRENT_FPOINT_REVIEW_RULE);
        if (rwMainList != null) {
            for (Object d : rwMainList) {
                Object[] ruleTemp = (Object[]) d;
                Integer reviewCampaignId = (Integer) ruleTemp[0];
                String actionName = (String) ruleTemp[1];
                String reviewType = (String) ruleTemp[2];
                Double actionValue = ((Integer) ruleTemp[5]).doubleValue();
                Double minActionValue = ((Integer) (ruleTemp[6] == null ? 0 : ruleTemp[6])).doubleValue();
                Double maxActionValue = ((Integer) (ruleTemp[7] == null ? MAX_VALUE : ruleTemp[7])).doubleValue();
                Date fromDate = (Date) ruleTemp[8];
                Date toDate = (Date) ruleTemp[9];

                //Process main fpoint reward for reviewId complete
                List<Object> reviewList = dao.runNativeQuery(REVIEW_INFO
                        .replace("{{FROM_DATE}}", DATE_FORMAT.format(fromDate))
                        .replace("{{TO_DATE}}", DATE_FORMAT.format(toDate)));
                for (Object x : reviewList) {
                    CustomerFpointInfo cf = new CustomerFpointInfo();

                    Object[] reviewTemp = (Object[]) x;
                    String customerId = String.valueOf((Long) reviewTemp[0]);
                    String customerEmail = (String) reviewTemp[1];
                    String reviewId = String.valueOf((BigInteger) reviewTemp[2]);
                    //Buffer 20 extra character to avoid differeces between to way to count characters
                    Long numCharacter = (Long) reviewTemp[3] + 20;
                    Long numLike = (reviewTemp[4] == null) ? 0l : ((BigDecimal) reviewTemp[4]).longValue();
                    String productId = String.valueOf((Long) reviewTemp[5]);

                    // Get num current fpoint for customer entity
                    String getCustomerEntity = GET_CUSTOMER_ENTITY
                            .replace("{{CUSTOMER_ID}}", String.valueOf(customerId));
                    Double numCurrentFpoint = ((BigDecimal) dao.runNativeQueryGetSingleResult(getCustomerEntity)).doubleValue();

                    cf.setActionValue(actionValue);
                    cf.setReviewType(reviewType);
//                    cf.setActionType(actionType);
                    cf.setNumCurrentFpoint(numCurrentFpoint);
                    cf.setCustomerEmail(customerEmail);
                    cf.setCustomerId(customerId);
                    cf.setActionName(actionName);
                    cf.setMinRuleValue(minActionValue);
                    cf.setMaxRuleValue(maxActionValue);
                    cf.setReviewId(reviewId);
                    cf.setReviewCampaignId(reviewCampaignId);
                    cf.setNumCharacter(numCharacter);
                    cf.setNumLike(numLike);
                    cf.setProductId(productId);

                    // Check exist reviewId fpoint rewarded
                    String checkExist = CHECK_EXIST_REVIEW_REWARDED
                            .replace("{{REVIEW_CAMPAIGN_ID}}", String.valueOf(reviewCampaignId))
                            .replace("{{PRODUCT_ID}}", productId)
                            .replace("{{CUS_ID}}", customerId);

                    Long numberProcess = ((Long) dao.getResultAsObject(checkExist));
                    Long numberProcessValue = numberProcess == null ? 0l : numberProcess.longValue();

                    if (numberProcessValue > 0) {
                        System.out.println("\nReview Id: " + reviewId + " bo qua vi da duoc xu ly\n");
                        continue;
                    }

                    if (matchReviewRule(cf) == true) {
                        //Match review fpoint reward rule. Process with action for this review
                        applyActionForThisReview(cf);
                        System.out.println("\nReview Id: " + reviewId + " thoa man rule " + reviewCampaignId + "!!!\n");
                    } else {
                        System.out.println("\nReview Id: " + reviewId + " khong thoa man rule " + reviewCampaignId + " !!!\n");
                    }
                }
            }
        }
    }

    
}
