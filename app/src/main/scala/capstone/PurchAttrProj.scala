package capstone

import java.sql.Timestamp

case class PurchAttrProj(purchaseId: String,
                         purchaseTime: Timestamp,
                         billingCost: Double,
                         isConfirmed: Boolean,
                         // a session starts with app_open event and finishes with app_close
                         sessionId: String,
                         campaignId: String, // derived from app_open#attributes#campaign_id
                         channelId: String // derived from app_open#attributes#channel_id
                        )
