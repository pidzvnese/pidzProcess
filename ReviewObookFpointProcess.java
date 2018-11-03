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
import com.fahasa.fpointprocess.dto.RedeemInfomation;
import com.fahasa.fpointprocess.model.Action;
import com.fahasa.fpointprocess.model.Variables;
import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author phongnh92
 */
public class ReviewObookFpointProcess {

    private static DAO dao;

    public static final Integer MAX_VALUE = 10000;
    public static final String REWARDED_STATUS = "rewarded";
    public static final String NOT_REWARDED_STATUS = "not rewarded";

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
            + "  a.customerId,\n"
            + "  a.reviewerEmail as customerEmail,\n"
            + "  a.review_id,\n"
            + "  character_length(replace(replace(replace(a.review, \" \", ''), \"\\n\", \"\"), \"\\r\", \"\")) as numberCharacter,\n"
            + "  sum(a.type = 'like')                                                               as numLike,\n"
            + "  a.entity_pk_value as productId,\n"
            + "  a.author_id,\n"
            + "  a.editor_type\n"
            + "from\n"
            + "  (select\n"
            + "     ce.entity_id as customerId,\n"
            + "     el.author    as reviewerEmail,\n"
            + "     el.review_id,\n"
            + "     el.review,\n"
            + "     ra.type,\n"
            + "     el.author_id,\n"
            + "     e.editor_type,\n"
            + "     r.entity_pk_value\n"
            + "   from fhs_review_editor_log el\n"
            + "     join fhs_review r on r.review_id = el.review_id\n"
            + "     join fhs_review_editor e on e.customer_id = el.customer_id\n"
            + "     left join fhs_customer_entity ce on ce.email = el.author\n"
            + "     left join fhs_reviews_action ra on ra.review_id = el.review_id and\n"
            + "             convert_tz(ra.created_at, '+0:00','+7:00') between '{{FROM_DATE}}' and '{{TO_DATE}}'\n"
            + "   where r.status_id = 1\n"
            + "         and convert_tz(r.created_at, '+0:00', '+7:00') between '{{FROM_DATE}}' and '{{TO_DATE}}'\n"
            + "  ) a\n"
            + "group by a.review_id";

    public static String ADD_ACTION_LOG_QUERY = "insert into fhs_purchase_action_log (account, customer_id, \n"
            + "action, reviewCampaignId, value,  amountAfter, updateBy, lastUpdated, review_id, description, \n"
            + "amountBefore, type) values ('{{EMAIL}}', {{CUSTOMER_ID}}, '{{ACTION}}', {{REVIEW_CAMPAIGN_ID}} ,{{NUM}}, {{NUMAFTER}}, \n"
            + "'tp script', now(), {{REVIEW_ID}}, '{{DESC}}', \n"
            + "'{{NUMBEFORE}}', '{{REWARDTYPE}}');";

    //There should be MAX 1 campaign for each review Id
    public static String CHECK_EXIST_REVIEW_REWARDED = "select count(*)\n"
            + "from fhs_purchase_action_log\n"
            + "where product_id = {{PRODUCT_ID}}\n"
            + "and reviewCampaignId = {{REVIEW_CAMPAIGN_ID}}\n"
            + "and customer_id = {{CUS_ID}}";

    public static String CHECK_EXIST_INSERT_LOG = "select count(1) from fhs_review_reward_log\n"
            + "where author_id = {{AUTHOR_ID}}\n"
            + "and review_id = {{REVIEW_ID}}";

    public static String GET_CUSTOMER_ENTITY = "select fpoint\n"
            + " from fhs_customer_entity \n"
            + "where entity_id={{CUSTOMER_ID}}";

    public static String FPOINT_CUSTOMER_UPDATE = "update fhs_customer_entity set fpoint={{NUMFPOINT}} \n"
            + "where entity_id={{CUSTOMER_ID}}";

    public static String INSERT_EMAIL_LOG = "insert into fhs_review_reward_log(email, fhs_customer_id, author_id, review_id,status, redeem_code, created_at, editor_type) \n"
            + "VALUES ('{{EMAIL}}', '{{FHS_CUSTOMER_ID}}','{{AUTHOR_ID}}', '{{STATUS}}', {{REVIEW_ID}},'{{REDEEM_CODE}}', NOW(), '{{EDITOR_TYPE}}')";
    private static final Logger logger = Logger.getLogger(FPointProcess.class.getName());
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final String API_URL = "https://app.fahasa.com:88/redeem/generate";

    public static void main(String[] args) {
        dao = new DAO();
        ReviewObookFpointProcess reviewObook = new ReviewObookFpointProcess();
        reviewObook.reviewFpointProcess();
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
                    String reviewerEmail = (String) (reviewTemp[1] == null ? "" : reviewTemp[1]);
                    String reviewId = String.valueOf((Integer) reviewTemp[2]);
                    Long numLike = (Long) reviewTemp[4];
                    String productId = String.valueOf((Integer) reviewTemp[5]);
                    String authorId = String.valueOf((Integer) reviewTemp[6]);
                    String editorType = (String) reviewTemp[7];
                    //Buffer 20 extra character to avoid differeces between to way to count characters
                    Long numCharacter = (Long) reviewTemp[3] + 20;
                    cf.setActionValue(actionValue);
                    cf.setReviewType(reviewType);

                    cf.setCustomerEmail(reviewerEmail);
                    cf.setNumLike(numLike);
                    cf.setProductId(productId);
                    cf.setEditorType(editorType);
                    cf.setReviewType(reviewType);
                    cf.setCustomerId(customerId);
                    cf.setActionName(actionName);
                    cf.setMinRuleValue(minActionValue);
                    cf.setMaxRuleValue(maxActionValue);
                    cf.setReviewCampaignId(reviewCampaignId);
                    cf.setNumCharacter(numCharacter);
                    cf.setReviewId(reviewId);

                    //Match review fpoint reward rule. Process with action for this review
                    if (matchReviewRule(cf) == true) {
                        if (customerId != null || !customerId.isEmpty()) {
                            String getCustomerEntity = GET_CUSTOMER_ENTITY
                                    .replace("{{CUSTOMER_ID}}", String.valueOf(customerId));
                            Double numCurrentFpoint = ((BigDecimal) dao.runNativeQueryGetSingleResult(getCustomerEntity)).doubleValue();
                            cf.setNumCurrentFpoint(numCurrentFpoint);
                            String checkExist = CHECK_EXIST_REVIEW_REWARDED
                                    .replace("{{PRODUCT_ID}}", productId)
                                    .replace("{{REVIEW_CAMPAIGN_ID}}", String.valueOf(reviewCampaignId))
                                    .replace("{{CUS_ID}}", customerId);

                            Long numberProcess = ((Long) dao.getResultAsObject(checkExist));
                            Long numberProcessValue = numberProcess == null ? 0l : numberProcess;

                            if (numberProcessValue > 0) {
                                System.out.println("\nReview Id: " + reviewId + " bo qua vi da duoc xu ly\n");
                                continue;
                            }

                            applyActionForThisReview(cf);
                            System.out.println("\nReview Id: " + reviewId + " thoa man rule " + reviewCampaignId + "!!!\n");
                            dao.executeInsertNativeQuery(INSERT_EMAIL_LOG
                                    .replace("{{EMAIL}}", reviewerEmail)
                                    .replace("{{FHS_CUSTOMER_ID}}", customerId)
                                    .replace("{{AUTHOR_ID}}", authorId)
                                    .replace("{{STATUS}}", REWARDED_STATUS)
                                    .replace("{{REDEEM_CODE}}", "")
                                    .replace("{{EDITOR_TYPE}}", editorType)
                                    .replace("{{REVIEW_ID}}", reviewId));

                        } else {
                            String checkExist = CHECK_EXIST_INSERT_LOG
                                    .replace("{{AUTHOR_ID}}", authorId)
                                    .replace("{{REVIEW_ID}}", reviewId);
                            Long numberProcess = ((Long) dao.getResultAsObject(checkExist));
                            if (numberProcess > 0) {
                                System.out.println("Author id: " + authorId + " with review id: " + reviewId + " has been added into log!!!");
                                continue;
                            }
                            customerId = "";
                            try {
                                RedeemInfomation ri = new RedeemInfomation();
                                ri.setCampaignId(Variables.GET_REDEEM_CODE_CAMPAIGN_ID);
                                ri.setCreatedBy(Variables.GET_REDEEM_CODE_CREATED_BY);
                                ri.setDescription(Variables.GET_REDEEM_CODE_DESCRIPTION);
                                ri.setExpiredDate(Variables.GET_REDEEM_CODE_EXPIRED_DAY);
                                ri.setFpointRedeemValue(Variables.GET_REDEEM_CODE_FPOINT_VALUE);
                                ri.setFreeshipRedeemValue(Variables.GET_REDEEM_CODE_FREESHIP_VALUE);
                                ri.setHashKey(Variables.GET_REDEEM_CODE_HASH_KEY);
                                ri.setQuantityValue(Variables.GET_REDEEM_CODE_QUANTITY_VALUE);
                                String redeemCode = getRedeemCode(ri);
                                dao.executeInsertNativeQuery(INSERT_EMAIL_LOG
                                        .replace("{{EMAIL}}", reviewerEmail)
                                        .replace("{{FHS_CUSTOMER_ID}}", customerId)
                                        .replace("{{AUTHOR_ID}}", authorId)
                                        .replace("{{STATUS}}", NOT_REWARDED_STATUS)
                                        .replace("{{REDEEM_CODE}}", redeemCode)
                                        .replace("{{EDITOR_TYPE}}", editorType)
                                        .replace("{{REVIEW_ID}}", reviewId));
                            } catch (UnirestException ex) {
                                Logger.getLogger(ReviewObookFpointProcess.class.getName()).log(Level.SEVERE, null, ex);
                            }

                        }
                    }
                }
            }
        }
    }

    public boolean matchReviewRule(CustomerFpointInfo cf) {
        boolean isMatchRule = false;
        String reviewRuleType = cf.getReviewType();
        Double minActionValue = cf.getMinRuleValue();
        Double maxActionValue = cf.getMaxRuleValue();
        Long numCharacter = cf.getNumCharacter();
        Long numLike = cf.getNumLike();
        if (reviewRuleType.equals(Variables.TYPE_REVIEW_CHARACTER)) {
            if (numCharacter >= minActionValue && numCharacter <= maxActionValue) {
                isMatchRule = true;
            }
        } else if (reviewRuleType.equals(Variables.TYPE_REVIEW_LIKE)) {
            if (numLike >= minActionValue && numLike <= maxActionValue) {
                isMatchRule = true;
            }
        }
        return isMatchRule;
    }

    public String getRedeemCode(RedeemInfomation ri) throws UnirestException {
        String redeemCode = null;
        String hashKey = ri.getHashKey();
        Integer fpointValue = ri.getFpointRedeemValue();
        Integer freeshipValue = ri.getFreeshipRedeemValue();
        Integer quantity = ri.getQuantityValue();
        String expiredDate = ri.getExpiredDate();
        String desc = ri.getDescription();
        String createdBy = ri.getCreatedBy();
        Integer campaignId = ri.getCampaignId();
        String body = "{\"hashKey\": \"" + hashKey + "\"" + ","
                + "\"fpointValue\": \"" + fpointValue + "\"" + ",\"quantity\": \"" + quantity + "\"" + ",\"expiredDate\": \"" + expiredDate + "\"" + ","
                + "\"freeshipValue\": \"" + freeshipValue + "\"" + ",\"description\": \"" + desc + "\"" + ",\"createdBy\":\"" + createdBy + "\"" + ",\"campaignId\": \"" + campaignId + "\"" + "}";

        HttpResponse<JsonNode> jsonResponse = Unirest.post(API_URL)
                .header("accept", "application/json")
                .body(body)
                .asJson();
        String jsonStr = jsonResponse.getBody().getObject().toString(3);
        JSONObject objectJson = new JSONObject(jsonStr);
        JSONArray arrJson = objectJson.getJSONArray("listRedeem");
        for (int i = 0; i < arrJson.length(); i++) {
            redeemCode = arrJson.getString(0);
        }
        System.out.println(redeemCode);
        return redeemCode;
    }

    public void applyActionForThisReview(CustomerFpointInfo cf) {
        Double actionValue = cf.getActionValue();
        Double numCurrentFpoint = cf.getNumCurrentFpoint();
        double nextToBeRewardFpoint = numCurrentFpoint + actionValue;
        String logMessage = null;
        if (cf.getActionName().equals(Action.REVIEW_X_LIKE_GET_Y_FPOINT)) {
            logMessage = "Reward fpoint amount " + cf.getActionValue() + " for review: " + cf.getReviewId()
                    + ", with num like is: " + cf.getNumLike() + ". This is for review campaign rule id: " + cf.getReviewCampaignId();
        } else if (cf.getActionName().equals(Action.REVIEW_X_CHARACTER_GET_Y_FPOINT)) {
            logMessage = "Reward fpoint amount " + cf.getActionValue() + " for review: " + cf.getReviewId()
                    + ", with num character is: " + cf.getNumCharacter() + ". This is for campaign rule id: " + cf.getReviewCampaignId();
        }
        insertLogAndUpdateCE(cf, actionValue, nextToBeRewardFpoint, logMessage, Variables.TYPE_FPOINT);
    }

    public void insertLogAndUpdateCE(CustomerFpointInfo cf, double valueRewarded, double nextToBeReward, String logMessage, String rewardType) {
        dao = new DAO();
        Double numCurentFpoint = cf.getNumCurrentFpoint();

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
                .replace("{{REWARDTYPE}}", rewardType);

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
}
