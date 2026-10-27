-- ==============================================================================
-- SILVER LAYER - ONE BIG TABLE (OBT) STREAMING TABLE
-- Joins streaming staged rides with dimensional reference/mapping tables
-- Implements watermarking on booking_timestamp with 3 minutes delay
-- ==============================================================================

CREATE OR REFRESH STREAMING TABLE silver_obt
AS
SELECT
    -- Ride & Passenger Identifiers
    stg_rides.ride_id,
    stg_rides.confirmation_number,
    stg_rides.passenger_id,
    stg_rides.driver_id,
    stg_rides.vehicle_id,
    stg_rides.pickup_location_id,
    stg_rides.dropoff_location_id,

    -- Foreign Keys
    stg_rides.vehicle_type_id,
    stg_rides.vehicle_make_id,
    stg_rides.payment_method_id,
    stg_rides.ride_status_id,
    stg_rides.pickup_city_id,
    stg_rides.dropoff_city_id,
    stg_rides.cancellation_reason_id,

    -- Passenger Details
    stg_rides.passenger_name,
    stg_rides.passenger_email,
    stg_rides.passenger_phone,

    -- Driver Details
    stg_rides.driver_name,
    stg_rides.driver_rating,
    stg_rides.driver_phone,
    stg_rides.driver_license,

    -- Vehicle Details
    stg_rides.vehicle_model,
    stg_rides.vehicle_color,
    stg_rides.license_plate,
    map_vehicle_makes.vehicle_make,
    map_vehicle_types.vehicle_type,
    map_vehicle_types.description AS vehicle_type_description,
    map_vehicle_types.base_rate,
    map_vehicle_types.per_mile,
    map_vehicle_types.per_minute,

    -- Location Details
    stg_rides.pickup_address,
    stg_rides.pickup_latitude,
    stg_rides.pickup_longitude,
    stg_rides.dropoff_address,
    stg_rides.dropoff_latitude,
    stg_rides.dropoff_longitude,
    map_cities.city AS pickup_city,
    map_cities.state,
    map_cities.region,
    map_cities.updated_at AS city_updated_at,

    -- Ride Metrics & Timestamps
    stg_rides.distance_miles,
    stg_rides.duration_minutes,
    stg_rides.booking_timestamp,
    stg_rides.pickup_timestamp,
    stg_rides.dropoff_timestamp,

    -- Financial Measures
    stg_rides.base_fare,
    stg_rides.distance_fare,
    stg_rides.time_fare,
    stg_rides.surge_multiplier,
    stg_rides.subtotal,
    stg_rides.tip_amount,
    stg_rides.total_fare,

    -- Status & Feedback
    map_ride_statuses.ride_status,
    map_cancellation_reasons.cancellation_reason,
    map_payment_methods.payment_method,
    map_payment_methods.is_card,
    map_payment_methods.requires_auth,
    stg_rides.rating

FROM
    STREAM(LIVE.stg_rides)
    WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stg_rides
    LEFT JOIN LIVE.map_vehicle_makes ON stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id
    LEFT JOIN LIVE.map_vehicle_types ON stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id
    LEFT JOIN LIVE.map_ride_statuses ON stg_rides.ride_status_id = map_ride_statuses.ride_status_id
    LEFT JOIN LIVE.map_payment_methods ON stg_rides.payment_method_id = map_payment_methods.payment_method_id
    LEFT JOIN LIVE.map_cities ON stg_rides.pickup_city_id = map_cities.city_id
    LEFT JOIN LIVE.map_cancellation_reasons ON stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id;
