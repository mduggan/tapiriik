from tapiriik.settings import WEB_ROOT, GOOGLEDRIVE_CLIENT_ID, GOOGLEDRIVE_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType
from tapiriik.services.storage_service_base import StorageServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.oauth2 import OAuth2Client
from tapiriik.services.interchange import Activity
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity, ServiceException
from django.core.urlresolvers import reverse
import logging
import requests
import json
import time
from pytz import UTC

from .activitytypes import googlefit_to_atype, atype_to_googlefit

logger = logging.getLogger(__name__)
#
# com.google.location.sample    The user's current location.    Location
# latitude (float—degrees)
# longitude (float—degrees)
# accuracy (float—meters)
# altitude (float—meters)
#
# Full scope needed so that we can read files that user adds by hand
_OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/fitness.activity.read",
    "https://www.googleapis.com/auth/fitness.activity.write",
    "https://www.googleapis.com/auth/fitness.location.read",
    "https://www.googleapis.com/auth/fitness.location.write"]

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/auth"
GOOGLE_TOKEN_URL = "https://accounts.google.com/o/oauth2/token"
GOOGLE_REVOKE_URL = "https://accounts.google.com/o/oauth2/revoke"

API_BASE_URL = "https://www.googleapis.com/fitness/v1/users/me/"


class GoogleFitService(StorageServiceBase):
    ID = "googlefit"
    DisplayName = "Google Fit"
    DisplayAbbreviation = "GF"
    AuthenticationType = ServiceAuthenticationType.OAuth
    Configurable = True
    ReceivesStationaryActivities = False
    AuthenticationNoFrame = True

    _oaClient = OAuth2Client(GOOGLEDRIVE_CLIENT_ID, GOOGLEDRIVE_CLIENT_SECRET, GOOGLE_TOKEN_URL, tokenTimeoutMin=55)

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("oauth_redirect", kwargs={"service": self.ID})
        pass

    def GenerateUserAuthorizationURL(self, level=None):
        return_url = WEB_ROOT + reverse("oauth_return", kwargs={"service": self.ID})
        params = {"redirect_uri": return_url, "response_type": "code", "access_type": "offline", "client_id": GOOGLEDRIVE_CLIENT_ID, "scope": ' '.join(_OAUTH_SCOPES)}
        return requests.Request(url=GOOGLE_AUTH_URL, params=params).prepare().url

    def RetrieveAuthorizationToken(self, req, level):
        def fetchUid(tokenData):
            # TODO: decide on a good UID
            return tokenData["refresh_token"]

        return self._oaClient.retrieveAuthorizationToken(self, req, WEB_ROOT + reverse("oauth_return", kwargs={"service": "sporttracks"}), fetchUid)

    def RevokeAuthorization(self, serviceRec):
        self._oaClient.revokeAuthorization(serviceRec, GOOGLE_REVOKE_URL)

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        session = self._oaClient.session(serviceRecord)
        session_list_url = API_BASE_URL + "sessions"
        activities = []
        excluded = []

        page_token = None
        while True:
            params = {}
            if page_token:
                params["pageToken"] = page_token
            session_list = session.get(session_list_url, params=params).json()
            slist = session_list.get("session")
            if not session:
                break

            for s in slist:
                startTime = s.get('startTimeMillis')
                endTime = s.get('endTimeMillis')
                actType = googlefit_to_atype[s["activityType"]]
                name = s.get("name")
                notes = s.get("description")
                a = Activity(startTime=startTime, endTime=endTime, actType=actType, name=name, notes=notes, tz=UTC)
                activities.append(a)

            page_token = session_list.get("nextPageToken")
            if not exhaustive or not page_token:
                break

        return activities, excluded

    def _get_data(self, serviceRecord):
        """ Work in progress.. """
        session = self._oaClient.session(serviceRecord)
        datasource_list_url = API_BASE_URL + "dataSources"

        # TODO: send dataTypeName parameter to restrict to these types only (since we can't sync the others):
        #com.google.activity.sample
        #com.google.activity.segment Continuous time interval of a single activity.  Activity    activity (int—enum)
        #com.google.calories.consumed    Total calories consumed over a time interval.   Activity    calories (float—kcal)
        #com.google.calories.expended    Total calories expended over a time interval.   Activity    calories (float—kcal)
        #com.google.cycling.pedaling.cadence Instantaneous pedaling rate in crank revolutions per minute.    Activity    rpm (float—rpm)
        #com.google.cycling.wheel_revolution.rpm Instantaneous wheel speed.  Location    rpm (float—rpm)
        #com.google.distance.delta   Distance covered since the last reading.    Location    distance (float—meters)
        #com.google.heart_rate.bpm   Heart rate in beats per minute. Body    bpm (float—bpm)
        #com.google.location.sample  The user's current location.    Location    latitude (float—degrees)
        #com.google.power.sample Instantaneous power generated while performing an activity. Activity    watts (float—watts)
        #com.google.speed    Instantaneous speed over ground.    Location    speed (float—m/s)
        #com.google.step_count.cadence   Instantaneous cadence in steps per minute.  Activity    rpm (float—steps/min)
        #com.google.step_count.delta Number of new steps since the last reading. Activity    steps (int—count)

        data_sources = session.get(datasource_list_url).json().get("dataSource") or []

        # Fetch the last 30 days of activity by default
        now_stamp = time.time()
        end_stamp = (now_stamp + 24*3600)
        start_stamp = (now_stamp - 30*24*3600)
        range_id = "%d-%d" % (start_stamp*1000000, end_stamp*1000000)

        for source in data_sources:
            if source.get("type") == "derived":
                # Only check raw data
                continue
            source_id = source.get("dataStreamId")
            dataset_list_url = "%s/%s/datasets/%s" % (datasource_list_url, source_id, range_id)

            page_token = None

            while True:
                params = None
                if page_token:
                    params = {"pageToken": page_token}
                dataset_list = session.get(dataset_list_url, params=params).json()
