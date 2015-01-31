from tapiriik.settings import WEB_ROOT, DROPBOX_APP_KEY, DROPBOX_APP_SECRET, DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType
from tapiriik.services.storage_service_base import StorageServiceBase
from tapiriik.services.api import APIException, UserException, UserExceptionType
from tapiriik.database import cachedb, redis
from dropbox import client, rest, session
from django.core.urlresolvers import reverse
from datetime import timedelta
import logging
import pickle
logger = logging.getLogger(__name__)


class DropboxService(StorageServiceBase):
    ID = "dropbox"
    DisplayName = "Dropbox"
    DisplayAbbreviation = "DB"
    AuthenticationType = ServiceAuthenticationType.OAuth
    AuthenticationNoFrame = True  # damn dropbox, spoiling my slick UI
    Configurable = True

    ConfigurationDefaults = {"SyncRoot": "/", "UploadUntagged": False, "Format": "tcx", "Filename": "%Y-%m-%d_#NAME_#TYPE"}

    def GetClient(self, serviceRec):
        if serviceRec.Authorization["Full"]:
            sess = session.DropboxSession(DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET, "dropbox")
        else:
            sess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "app_folder")
        sess.set_token(serviceRec.Authorization["Key"], serviceRec.Authorization["Secret"])
        return client.DropboxClient(sess)

    def WebInit(self):
        self.UserAuthorizationURL = reverse("oauth_redirect", kwargs={"service": self.ID})

    def RequiresConfiguration(self, svcRec):
        return svcRec.Authorization["Full"] and ("SyncRoot" not in svcRec.Config or not len(svcRec.Config["SyncRoot"]))

    def GenerateUserAuthorizationURL(self, level=None):
        full = level == "full"
        if full:
            sess = session.DropboxSession(DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET, "dropbox")
        else:
            sess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "app_folder")

        reqToken = sess.obtain_request_token()
        redis.setex("dropbox:oauth:%s" % reqToken.key, pickle.dumps(reqToken), timedelta(hours=24))
        return sess.build_authorize_url(reqToken, oauth_callback=WEB_ROOT + reverse("oauth_return", kwargs={"service": "dropbox", "level": "full" if full else "normal"}))

    def RetrieveAuthorizationToken(self, req, level):
        tokenKey = req.GET["oauth_token"]

        redis_key = "dropbox:oauth:%s" % tokenKey
        token = redis.get(redis_key)
        assert token
        token = pickle.loads(token)
        redis.delete(redis_key)

        full = level == "full"
        if full:
            sess = session.DropboxSession(DROPBOX_FULL_APP_KEY, DROPBOX_FULL_APP_SECRET, "dropbox")
        else:
            sess = session.DropboxSession(DROPBOX_APP_KEY, DROPBOX_APP_SECRET, "app_folder")

        accessToken = sess.obtain_access_token(token)

        uid = int(req.GET["uid"])  # duh!
        return (uid, {"Key": accessToken.key, "Secret": accessToken.secret, "Full": full})

    def RevokeAuthorization(self, serviceRecord):
        pass  # :(

    def ConfigurationUpdating(self, svcRec, newConfig, oldConfig):
        from tapiriik.sync import Sync
        from tapiriik.auth import User
        if newConfig["SyncRoot"] != oldConfig["SyncRoot"]:
            Sync.ScheduleImmediateSync(User.AuthByService(svcRec), True)
            cachedb.dropbox_cache.update({"ExternalID": svcRec.ExternalID}, {"$unset": {"Structure": None}})

    def _raiseDbException(self, e):
        if e.status == 401:
            raise APIException("Authorization error - status " + str(e.status) + " reason " + str(e.error_msg) + " body " + str(e.body), block=True, user_exception=UserException(UserExceptionType.Authorization, intervention_required=True))
        if e.status == 507:
            raise APIException("Dropbox quota error", block=True, user_exception=UserException(UserExceptionType.AccountFull, intervention_required=True))
        raise APIException("API failure - status " + str(e.status) + " reason " + str(e.reason) + " body " + str(e.error_msg))

    def _folderRecurse(self, structCache, dbcl, path):
        hash = None
        existingRecord = [x for x in structCache if x["Path"] == path]
        children = [x for x in structCache if x["Path"].startswith(path) and x["Path"] != path]
        existingRecord = existingRecord[0] if len(existingRecord) else None
        if existingRecord:
            hash = existingRecord["Hash"]
        try:
            dirmetadata = dbcl.metadata(path, hash=hash)
        except rest.ErrorResponse as e:
            if e.status == 304:
                for child in children:
                    self._folderRecurse(structCache, dbcl, child["Path"])  # still need to recurse for children
                return  # nothing new to update here
            if e.status == 404:
                # dir doesn't exist any more, delete it and all children
                structCache[:] = (x for x in structCache if x != existingRecord and x not in children)
                return
            self._raiseDbException(e)
        if not existingRecord:
            existingRecord = {"Files": [], "Path": dirmetadata["path"]}
            structCache.append(existingRecord)

        existingRecord["Hash"] = dirmetadata["hash"]
        existingRecord["Files"] = []
        curDirs = []
        for file in dirmetadata["contents"]:
            if file["is_dir"]:
                curDirs.append(file["path"])
                self._folderRecurse(structCache, dbcl, file["path"])
            else:
                if not file["path"].lower().endswith(".gpx") and not file["path"].lower().endswith(".tcx"):
                    continue  # another kind of file
                existingRecord["Files"].append({"Rev": file["rev"], "Path": file["path"]})
        structCache[:] = (x for x in structCache if x["Path"] in curDirs or x not in children)  # delete ones that don't exist

    def EnumerateFiles(self, svcRec, dbcl, root, cache):
        if "Structure" not in cache:
            cache["Structure"] = []
        self._folderRecurse(cache["Structure"], dbcl, root)

        for dir in cache["Structure"]:
            for file in dir["Files"]:
                path = file["Path"]
                if svcRec.Authorization["Full"]:
                    relPath = path.replace(root, "", 1)
                else:
                    relPath = path.replace("/Apps/tapiriik/", "", 1)  # dropbox api is meh api
                yield (path, relPath, path, file["Rev"])

    def GetFileContents(self, serviceRecord, dbcl, path, storageid, cache):
        try:
            f, metadata = dbcl.get_file_and_metadata(path)
        except rest.ErrorResponse as e:
            self._raiseDbException(e)

        activityData = f.read()
        return activityData, metadata["rev"]

    def PutFileContents(self, serviceRecord, dbcl, path, contents, cache):
        try:
            metadata = dbcl.put_file(path, contents)
        except rest.ErrorResponse as e:
            self._raiseDbException(e)

        return metadata["rev"]

    def MoveFile(self, serviceRecord, dbcl, path, destPath, cache):
        dbcl.file_move(path, path.replace(".tcx", ".tcx.summary-data"))

    def ServiceCacheDB(self):
        return cachedb.dropbox_cache

    def SyncRoot(self, svcRec):
        if not svcRec.Authorization["Full"]:
            syncRoot = "/"
        else:
            syncRoot = svcRec.Config["SyncRoot"]
        return syncRoot
