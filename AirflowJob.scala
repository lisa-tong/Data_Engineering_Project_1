package Flow.SparkJob

import Flow.SparkJob.Domain._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object AirbnbJob {
    def run(params: SparkParams) {
        val spark = SparkSession.builder.appName(params.parser).getOrCreate
        
        // the map and add to spark.read
        val data = spark.read.option("multiLine", true).option("quote", "\"").option("escape", "\"").option("header", "true").option("delimiter", ",").option("treatEmptyValuesAsNulls","true").csv(params.inPath)

        // Remove unnecessary data

        val newData = data.drop("scrape_id", "last_scraped", "experiences", "neighborhood", "notes", "interaction", "house_rules", "thumbnail_url", "medium_url", "picture_url", "xl_picture_url", "xl_picture_url", "host_about", "host_response_time", "host_response_rate", "host_acceptance_rate", "host_thumbnail_url", "host_is_superhost", "host_picture_url", "host_listings_count", "host_verifications", "host_has_profile_pic", "host_identity_verified", "street", "host_neighborhood", "neighbourhood_group_cleansed", "market", "smart_location", "country_code", "country", "accommodates", "bed_type", "amenities", "square_feet", "price", "weekly_price", "monthly_price", "security_deposit", "cleaning_fee", "guests_included", "extra_people", "minimum_nights", "maximum_nights", "minimum_minimum_nights", "maximum_minimum_nights", "minimum_maximum_nights", "maximum_maximum_nights", "minimum_nights_avg_ntm", "maximum_nights_avg_ntm", "calendar_updated", "has_availability", "availability_30", "availability_60", "availability_90", "availability_365", "calendar_last_scraped", "number_of_reviews", "number_of_reviews_ltm", "review_scores_rating", "review_scores_accuracy", "review_scores_cleanliness", "review_scores_checkin", "review_scores_communication", "review_scores_location", "review_scores_value", "requires_license", "license", "jurisdiction_names", "instant_bookable", "is_business_travel_ready", "cancellation_policy", "require_guest_profile_picture", "require_guest_phone_verification", "calculated_host_listings_count", "calculated_host_listings_count_entire_homes", "calculated_host_listings_count_private_rooms", "calculated_host_listings_count_shared_rooms", "reviews_per_month")


        // TODO: Filter property_type for apartment and condos.
        val filter = newData.where(newData("property_type") === "Condominium" && newData("room_type") === "Entire home/apt")

        // add a new column so that end user can identify which app generates the data
        val resultDF = filter.withColumn("source", lit("filtered_lt"))

        resultDF.write
        .partitionBy(params.partitionColumn)
        .options(params.outOptions)
        .format(params.outFormat)
        .mode(params.saveMode)
        .save(params.outPath)

    }
}
