package capstone

import java.sql.Timestamp

case class PurchasesProj(purchaseId: String,
                         purchaseTime: Timestamp,
                         billingCost: Double,
                         isConfirmed: Boolean)
