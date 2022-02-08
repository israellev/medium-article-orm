class UsersSportActivityDbService:
    __db = None

    def __init__(self, db):
        self.__db = db

    def insert_new_activity(self, account_id, user_id, service_type, activity):
        service_user_id = activity.get("service_user_id")  # VARCHAR(60)
        service_activity_id = activity.get("service_activity_id")  # VARCHAR(60)
        source_type = activity.get("source_type")  # CHAR(30)
        source_id = activity.get("source_id")  # VARCHAR(60)
        device_name = activity.get("device_name")  # VARCHAR(80)
        activity_type = activity.get("activity_type")  # CHAR(30)
        activity_name = activity.get("activity_name")  # VARCHAR(80)
        start_time_in_seconds = activity.get("start_time_in_seconds")  # int
        start_time = activity.get("start_time")  # int
        utc_offset = activity.get("utc_offset")  # int
        duration = activity.get("duration")  # int
        moving = activity.get("moving")  # int
        distance = activity.get("distance")  # float
        avg_speed_in_kph = float(activity.get("avg_speed_in_mps")) * 3.6 if activity.get("avg_speed_in_mps") else None  # float
        max_speed_in_kph = float(activity.get("max_speed_in_mps")) * 3.6 if activity.get("max_speed_in_mps") else None  # float
        total_elevation_in_meters = activity.get("total_elevation_in_meters")  # float
        steps = activity.get("steps")  # int
        avg_steps_per_minute = activity.get("avg_steps_per_minute")  # float
        max_steps_per_minute = activity.get("max_steps_per_minute")  # float
        avg_heart_rate_per_minute = activity.get("avg_heart_rate_per_minute")  # float
        max_heart_rate_per_minute = activity.get("max_heart_rate_per_minute")  # float
        starting_latitude = activity.get("starting_latitude")  # float
        starting_longitude = activity.get("starting_longitude")  # float
        avg_temp = activity.get("avg_temp")  # float
        calories = activity.get("calories")  # int
        external_id = activity.get("external_id")  # VARCHAR(80)
        sql = """INSERT INTO users_sport_activity (account_id, user_id, service_type, service_user_id, 
        service_activity_id, source_id, source_type, 
        device_name, activity_type, activity_name, start_time_in_seconds, start_time, utc_offset, duration, 
        moving, distance, avg_speed_in_kph, max_speed_in_kph, total_elevation_in_meters, steps, 
        avg_steps_per_minute, max_steps_per_minute, avg_heart_rate_per_minute, max_heart_rate_per_minute, 
        starting_latitude, starting_longitude, avg_temp, calories, external_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        params = (
            account_id, user_id, service_type, service_user_id, service_activity_id, source_id, source_type,
            device_name, activity_type,
            activity_name, start_time_in_seconds, start_time, utc_offset, duration, moving, distance,
            avg_speed_in_kph, max_speed_in_kph, total_elevation_in_meters, steps, avg_steps_per_minute,
            max_steps_per_minute, avg_heart_rate_per_minute, max_heart_rate_per_minute, starting_latitude,
            starting_longitude, avg_temp, calories, external_id
        )
        return self.__db.insert(sql, safe_params=params)
    
    def get_user_activities(self, app_type, user_id):
        sql = """SELECT service_type, activity_type, activity_name, start_time_in_seconds, moving, distance, avg_temp, 
        avg_heart_rate_per_minute FROM users_sport_activity
        WHERE account_id = %s AND user_id = %s
        """
        params = [app_type, user_id]
        rows = self.__db.select(sql, safe_params=params + params) or []
        result_list = []
        for row in rows:
            result_list.append({
                "service_type": row[0],
                "activity_type": row[1],
                "activity_name": row[2],
                "start_time": row[3],
                "during": row[4],
                "distance": row[5],
                "avg_temp": row[6],
                "avg_heart_rate": row[7],
            })
        return result_list

    def update_activity_name(self, service_activity_id, new_activity_name):
        sql = """UPDATE users_sport_activity SET activity_name = %s 
        WHERE service_type = "strava" AND service_activity_id = %s"""
        return self.__db.update(sql, safe_params=(new_activity_name, service_activity_id))

