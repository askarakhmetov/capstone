package capstone

import java.sql.Timestamp

case class MobAppClickProj(userId: String,
                           eventId: String,
                           eventTime: Timestamp,
                           eventType: String,
                           attributes: Option[Map[String, String]])
