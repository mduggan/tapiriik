from tapiriik.settings import WEB_ROOT, GOOGLEDRIVE_CLIENT_ID, GOOGLEDRIVE_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType
from tapiriik.services.storage_service_base import StorageServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.oauth2 import OAuth2Client
from tapiriik.services.interchange import UploadedActivity, Waypoint, WaypointType, Location, Lap
from tapiriik.services.api import APIException, UserException, UserExceptionType, APIExcludeActivity, ServiceException
from tapiriik.database import cachedb
from django.core.urlresolvers import reverse
import logging
import requests
import json
from datetime import timedelta, datetime
import pytz
import calendar

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

# See https://developers.google.com/fit/rest/v1/data-types
SUPPORTED_DATATYPES = [
    # "com.google.activity.sample",
    # "com.google.activity.segment", # TODO: should support this as Lap?
    "com.google.calories.expended",  # I presume calories.consumed actually means "ingested", otherwise it's the same thing??
    "com.google.cycling.pedaling.cadence",
    "com.google.distance.delta",
    "com.google.heart_rate.bpm",
    "com.google.location.sample",
    "com.google.power.sample",
    "com.google.speed",
    "com.google.step_count.cadence",
    # "com.google.step_count.delta", # TODO: would be nice to support this?
]

APP_NAME = "com.tapiriik.sync"


def _fpVal(f):
    return {'fpVal': f}

class GoogleFitService(StorageServiceBase):
    ID = "googlefit"
    DisplayName = "Google Fit"
    DisplayAbbreviation = "GF"
    AuthenticationType = ServiceAuthenticationType.OAuth
    Configurable = True
    ReceivesStationaryActivities = False
    AuthenticationNoFrame = True
    SupportsHR = SupportsCalories = SupportsCadence = SupportsPower = True
    SupportsTemp = False  # could created a custom data type, but not supported by default..
    SupportedActivities = atype_to_googlefit.keys()
    GlobalRateLimits = [(timedelta(days=1), 86400)]

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

    def DeleteCachedData(self, serviceRecord):
        cachedb.googlefit_source_cache.remove({"ExternalID": serviceRecord.ExternalID})

    def DownloadActivityList(self, serviceRecord, exhaustive=False):
        session = self._oaClient.session(serviceRecord)
        session_list_url = API_BASE_URL + "sessions"
        activities = []
        excluded = []

        sources = self._getDataSources(serviceRecord, session, forceRefresh=True)

        if not sources:
            # No sources of interest, don't bother listing sessions (save an API call)
            return activities, excluded

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
                act = UploadedActivity()
                if 'application' not in s:
                    continue
                act.StartTime = pytz.utc.localize(datetime.utcfromtimestamp(float(s["startTimeMillis"])/1000))
                act.EndTime = pytz.utc.localize(datetime.utcfromtimestamp(float(s["endTimeMillis"])/1000))
                act.Type = googlefit_to_atype[s["activityType"]]
                # FIXME; this is not really right..
                act.TZ = pytz.UTC
                act.Notes = s.get("description")
                act.Name = s.get("name")
                act.ServiceData = {'Id': s.get('id')}
                appdata = s['application']
                act.ServiceData['ApplicationPackage'] = appdata.get('packageName')
                act.ServiceData['ApplicationVersion'] = appdata.get('version')
                act.ServiceData['ApplicationName'] = appdata.get('name')
                activities.append(act)

            page_token = session_list.get("nextPageToken")
            if not exhaustive or not page_token:
                break

        return activities, excluded

    def _getDataSources(self, serviceRecord, session, forceRefresh=False):
        datasource_url = API_BASE_URL + "dataSources"
        raw_sources = None
        if not forceRefresh:
            raw_sources = cachedb.googlefit_source_cache.find_one({"ExternalID": serviceRecord.ExternalID})

        if not raw_sources:
            raw_sources = session.get(datasource_url, param={'dataTypeName': SUPPORTED_DATATYPES}).text
            cachedb.googlefit_source_cache.update({"ExternalID": serviceRecord.ExternalID}, raw_sources)

        return json.loads(raw_sources).get("dataSource")

    def _toUTCNano(ts):
        return calendar.timegm(ts.utctimetuple()) * int(1e9)

    def _toUTCMilli(ts):
        return calendar.timegm(ts.utctimetuple()) * int(1e3)

    def DownloadActivity(self, serviceRecord, activity):
        session = self._oaClient.session(serviceRecord)
        # If it  came from DownloadActivityList it will have this..
        assert 'ApplicationPackage' in activity.ServiceData
        dataset_url = API_BASE_URL + "dataSources/%s/datasets/%d-%d"

        start_nano = self._toUTCNano(activity.StartTime)
        end_nano = self._toUTCNano(activity.EndTime)

        # Grab the streams from the same app as this session:
        sources = self._getDataSources(serviceRecord)
        sources = filter(sources, lambda x: x['application']['packageName'] == activity.ServiceData['ApplicationPackage'], sources)

        # Combine the data for each point from each stream.
        waypoints = {}
        for source in sources:
            streamid = source["dataStreamId"]
            #sourcedatatype = source["dataType"]["name"]
            logger.debug("fetch data for stream %s" % streamid)
            source_url = dataset_url % (streamid, start_nano, end_nano)

            data = session.get(source_url)

            try:
                data = data.json()
            except:
                raise APIException("JSON parse error")

            points = data.get('point')
            if not points:
                continue

            for point in points:
                pointType = point["dataTypeName"]
                startms = int(point["startTimeNanos"] / 1000000)
                wp = waypoints.get(startms)
                if wp is None:
                    wp = waypoints[startms] = Waypoint(datetime.utcfromtimestamp(startms))

                values = point["value"]
                if pointType == "com.google.location.sample":
                    wp.Location = Location()
                    wp.Location.Latitude = values[0]["fpVal"]
                    wp.Location.Longitude = values[1]["fpVal"]
                    # values[2] is accuracy
                    wp.Location.Altitude = values[3]["fpVal"]
                elif pointType == "com.google.heart_rate.bpm":
                    wp.HR = values[0]["fpVal"]
                elif pointType == "com.google.calories.expended":
                    wp.Calories = values[0]["fpVal"]
                elif pointType == "com.google.cycling.pedaling.cadence" or pointType == "com.google.cycling.wheel_revolution.rpm":
                    wp.Cadence = values[0]["fpVal"]
                elif pointType == "com.google.step_count.cadence":
                    wp.RunCadence = values[0]["fpVal"]
                elif pointType == "com.google.speed":
                    wp.Speed = values[0]["fpVal"]
                elif pointType == "com.google.distance.delta":
                    wp.Distance = values[0]["fpVal"]
                elif pointType == "com.google.power.sample":
                    wp.Power = values[0]["fpVal"]
                elif pointType == "com.google.step_count.delta":
                    # TODO: use this data
                    # steps = values[0]["intVal"]
                    logging.debug("Step count delta (not supported..)")
                else:
                    logging.info("Unexpected point data type %s.." % pointType)

        # Sort all points by time
        wpkeys = waypoints.keys()
        wpkeys.start()
        lap = Lap(startTime=activity.StartTime, endTime=activity.EndTime)  # no laps in google fit.. just make one.
        activity.Laps = [lap]
        lap.Waypoints = [waypoints[x] for x in wpkeys]
        # A bit approximate..
        lap.Waypoints[0].Type = WaypointType.Start
        lap.Waypoints[-1].Type = WaypointType.End

        return activity

    def _ensureSourcesExist(self, serviceRecord, session, sources):
        datasource_url = API_BASE_URL + "dataSources"

        tap_sources = filter(lambda x: x["application"].get("name") == APP_NAME, sources)
        added = False

        for tname in SUPPORTED_DATATYPES:
            if len(filter(lambda x: x["dataType"]["name"] == SUPPORTED_DATATYPES), tap_sources):
                continue
            # Source doesn't exist.. create it
            description = {
                "application": {"name": APP_NAME},
                # TODO: do I really have to describe the fields for the default types?
                "dataType": {"name": tname},
                "type": "raw",
            }
            response = session.post(datasource_url, data=description)

            newdesc = response.json()
            sources.append(newdesc)
            added = True
        if added:
            raw_sources = json.dumps(sources)
            cachedb.googlefit_source_cache.update({"ExternalID": serviceRecord.ExternalID}, raw_sources)
        return sources

    def UploadActivity(self, serviceRecord, activity):
        session = self._oaClient.session(serviceRecord)
        session_url = API_BASE_URL + "sessions"
        sources = self._getDataSources(serviceRecord)
        sources = self._ensureSourcesExist(session, sources)

        # Create a session representing this activity
        startms = self._toUTCMilli(activity.StartTime)
        endms = self._toUTCMilli(activity.EndTime)
        modms = self._toUTCMilli(datetime.now())  # TODO: Is this ok?
        sess_data = {
            "id": str(startms),
            "name": activity.Name,
            "description": activity.Notes,
            "startTimeMillis": startms,
            "endTimeMillis": endms,
            "modifiedTimeMillis": modms,
            "application": {"name": APP_NAME},
            "activityType": atype_to_googlefit[activity.Type]
        }
        result = session.put(session_url, data=sess_data)
        # TODO: should check this matches what we put in.
        outr = result.json()

        # Split the activity into data streams, as we have to upload each one individually
        locs = []
        hr = []
        cals = []
        cadence = []
        runcad = []
        speed = []
        dist = []
        power = []

        for lap in activity.Laps:
            for wp in lap.Waypoints:
                wp_nanos = self._toUTCNano(wp.Timestamp)
                if wp.Location is not None:
                    # TODO: Just put in 1m accuracy here.. what else to do?
                    locs.append((wp_nanos, [_fpVal(wp.Location.Latitude), _fpVal(wp.Location.Longitude), _fpVal(1), _fpVal(wp.Location.Altitude)]))
                if wp.HR is not None:
                    hr.append(wp_nanos, [_fpVal(wp.HR)])
                if wp.Calories is not None:
                    cals.append(wp_nanos, [_fpVal(wp.Calories)])
                if wp.Cadence is not None:
                    cadence.append(wp_nanos, [_fpVal(wp.Cadence)])
                if wp.RunCadence is not None:
                    runcad.append(wp_nanos, [_fpVal(wp.RunCadence)])
                if wp.Speed is not None:
                    speed.append(wp_nanos, [_fpVal(wp.RunCadence)])
                if wp.Distance is not None:
                    dist.append(wp_nanos, [_fpVal(wp.Distance)])
                if wp.Power is not None:
                    power.append(wp_nanos, [_fpVal(wp.Power)])

        dataset_url = API_BASE_URL + "dataSources/%s/datasets/%d-%d"
        data_types = [
            (locs, "com.google.location.sample"),
            (hr, "com.google.heart_rate.bpm"),
            (cals, "com.google.calories.expended"),
            (cadence, "com.google.cycling.pedaling.cadence"),
            (runcad, "com.google.step_count.cadence"),
            (speed, "com.google.speed"),
            (dist, "com.google.distance.delta"),
            (power, "com.google.power.sample"), ]

        for points, tname in data_types:
            if not points:
                continue
            s = filter(lambda x: x["application"].get("name") == APP_NAME and x["dataType"]["name"] == tname, sources)
            if not s or "dataStreamId" not in s[0]:
                raise APIException("Data source not created correctly!")
            streamId = s[0]["dataStreamId"]

            def make_point(x):
                return {"dataTypeName": tname, "startTimeNanos": x[0], "endTimeNanos": x[0], "value": x[1:]}
            point_list = [make_point(x) for x in points]

            put_data = {"dataSourceId": streamId, "minStartTimeNs": points[0][0], "maxEndTimeNs": points[-1][0], "point": point_list}
            result = session.patch(dataset_url % (streamId, points[0][0], points[-1][0]), data=put_data)

            result_json = result.json()
            # TODO: check the return value.

        return str(startms)
