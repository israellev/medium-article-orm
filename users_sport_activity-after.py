from modules.utils_time import UtilsTime
from modules.utils_sql import UtilsSql
from db_services.dto_base import DtoBase
from db_services.app_oauth1_users import AppOAuth1UsersDbService
from db_services.app_oauth_users import AppOAuthUsersDbService


class SPORT_ACTIVITY_DTO:
    ID = "id"
    ACCOUNT_ID = "account_id"
    USER_ID = "user_id"
    SERVICE_TYPE = "service_type"
    SERVICE_USER_ID = "service_user_id"
    SERVICE_ACTIVITY_ID = "service_activity_id"
    SOURCE_ID = "source_id"
    SOURCE_TYPE = "source_type"
    DEVICE_NAME = "device_name"
    ACTIVITY_TYPE = "activity_type"
    ACTIVITY_NAME = "activity_name"
    START_TIME_IN_SECONDS = "start_time_in_seconds"
    START_TIME = "start_time"
    UTC_OFFSET = "utc_offset"
    DURATION = "duration"
    MOVING = "moving"
    DISTANCE = "distance"
    AVG_SPEED_IN_KPH = "avg_speed_in_kph"
    MAX_SPEED_IN_KPH = "max_speed_in_kph"
    TOTAL_ELEVATION_IN_METERS = "total_elevation_in_meters"
    STEPS = "steps"
    AVG_STEPS_PER_MINUTE = "avg_steps_per_minute"
    MAX_STEPS_PER_MINUTE = "max_steps_per_minute"
    AVG_HEART_RATE_PER_MINUTE = "avg_heart_rate_per_minute"
    MAX_HEART_RATE_PER_MINUTE = "max_heart_rate_per_minute"
    STARTING_LATITUDE = "starting_latitude"
    STARTING_LONGITUDE = "starting_longitude"
    AVG_TEMP = "avg_temp"
    CALORIES = "calories"
    EXTERNAL_ID = "external_id"
    CATEGORY_ID = "category_id"
    IS_PUSH_SENT = "is_push_sent"
    IS_DELETED = "is_deleted"


class SERVICE_TYPES:
    GARMIN = "garmin"
    STRAVA = "strava"
    MANUAL = "manual"
    APPLE = "apple"
    GOOGLE = "google"


APP_SPORT_SERVICE_TYPES = [SERVICE_TYPES.MANUAL, SERVICE_TYPES.APPLE, SERVICE_TYPES.GOOGLE]
CATEGORY_ID_LIST = ["ball_games", "cycling", "gym", "other", "running", "swimming", "walking", "yoga"]


class UsersSportActivityDbService(DtoBase):
    TABLE_NAME = "users_sport_activity"
    CLASS_DTO = SPORT_ACTIVITY_DTO

    def __init__(self, db, app_type):
        DtoBase.__init__(self, db)
        self.BASE_WHERE = {SPORT_ACTIVITY_DTO.ACCOUNT_ID: app_type, SPORT_ACTIVITY_DTO.IS_DELETED: False}
        self._app_type = app_type
        self._today = UtilsTime.get_start_of_today()
        self._oauth1_users_db_service = AppOAuth1UsersDbService(db)
        self._oauth_users_db_service = AppOAuthUsersDbService(db)

    def insert_new_activity(self, activity):
        if not activity.get(SPORT_ACTIVITY_DTO.CATEGORY_ID):
            activity[SPORT_ACTIVITY_DTO.CATEGORY_ID] = self.__get_category_id(activity)
        return self._insert_dto_obj(activity)

    def get_user_activities(self, user_id):
        return self._get_dto_list(user_id=user_id)

    def get_user_activities(self, user_id, hours_from_utc):
        # handle deleted users and duplicate activities
        local_time_column = UtilsSql.get_column_in_local_time(hours_from_utc, time_column="start_time")
        active_garmin_user_tokens_sql_query = self._oauth1_users_db_service.get_active_garmin_user_tokens_sql_query()
        active_strava_user_ids_sql_query = self._oauth_users_db_service.get_active_strava_user_ids_sql_query()
        where = """WHERE account_id = %s AND {local_time_column} > %s 
        {user_id_filter}
        AND NOT is_deleted
        AND ((service_type = "{strava}" AND service_user_id IN ({deleted_strava_user_ids_sql_query}))
        OR (service_type = "{garmin}" AND  service_user_id IN ({deleted_garmin_user_tokens_sql_query}))
        OR service_type IN ({app_source_types})) AND NOT is_deleted
        """.format(
            local_time_column=local_time_column,
            user_id_filter="AND user_id = %s".format(user_id) if user_id is not None else "",
            deleted_garmin_user_tokens_sql_query=active_garmin_user_tokens_sql_query,
            deleted_strava_user_ids_sql_query=active_strava_user_ids_sql_query,
            strava=SERVICE_TYPES.STRAVA,
            garmin=SERVICE_TYPES.GARMIN,
            app_source_types=UtilsSql.get_string_from_list(APP_SPORT_SERVICE_TYPES)
        )
        params = [self._app_type, self._today]
        if user_id:
            params.append(user_id)
        activities_list = self._get_dto_list_where(where, params + params)
        return activities_list

    def get_activity_by_source_id(self, source_type, source_id):
        return self._get_dto(source_type=source_type, source_id=source_id)

    def get_activity_by_service_activity_id(self, service_type, service_activity_id):
        return self._get_dto(service_type=service_type, service_activity_id=service_activity_id)

    def update_strava_activity_name(self, service_type, service_activity_id, new_name):
        where_dict = {
            SPORT_ACTIVITY_DTO.SERVICE_TYPE: service_type,
            SPORT_ACTIVITY_DTO.SERVICE_ACTIVITY_ID: service_activity_id,
        }
        return self._update_dto(where_dict, activity_name=new_name)
