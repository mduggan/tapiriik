from tapiriik.settings import WEB_ROOT, GOOGLEDRIVE_CLIENT_ID, GOOGLEDRIVE_CLIENT_SECRET
from tapiriik.services.service_base import ServiceAuthenticationType
from tapiriik.services.storage_service_base import StorageServiceBase
from tapiriik.services.service_record import ServiceRecord
from tapiriik.services.api import APIException, ServiceExceptionScope, UserException, UserExceptionType, APIExcludeActivity, ServiceException
from tapiriik.database import cachedb
from oauth2client.client import OAuth2WebServerFlow, OAuth2Credentials
from apiclient.discovery import build
from apiclient.http import MediaInMemoryUpload
from apiclient import errors
from oauth2client import GOOGLE_REVOKE_URI
from django.core.urlresolvers import reverse
import logging
import httplib2
import requests
import json

logger = logging.getLogger(__name__)

# Full scope needed so that we can read files that user adds by hand
OAUTH_SCOPE = 'https://www.googleapis.com/auth/drive'

# Mimetypes to use when uploading, keyed by extension
MIMETYPES = {
    'gpx': 'application/gpx+xml',
    'tcx': 'application/vnd.garmin.tcx+xmle'
}

# Mimetype given to folders on google drive.
FOLDER_MIMETYPE = 'application/vnd.google-apps.folder'

class GoogleDriveService(StorageServiceBase):
    ID = "googledrive"
    DisplayName = "Google Drive"
    DisplayAbbreviation = "GD"
    AuthenticationType = ServiceAuthenticationType.OAuth
    Configurable = True
    ReceivesStationaryActivities = False
    AuthenticationNoFrame = True

    def _oauthFlow(self):
        return_url = WEB_ROOT + reverse("oauth_return", kwargs={"service": self.ID})
        flow = OAuth2WebServerFlow(GOOGLEDRIVE_CLIENT_ID, GOOGLEDRIVE_CLIENT_SECRET, OAUTH_SCOPE,
                                   redirect_uri=return_url)
        return flow

    def GetClient(self, serviceRec):
        credentials = OAuth2Credentials.from_json(serviceRec.Authorization["Credentials"])
        http = httplib2.Http()
        http = credentials.authorize(http)
        drive_service = build('drive', 'v2', http=http)
        return drive_service

    def WebInit(self):
        self.UserAuthorizationURL = WEB_ROOT + reverse("oauth_redirect", kwargs={"service": self.ID})
        pass

    def GenerateUserAuthorizationURL(self, level=None):
        flow = self._oauthFlow()
        return flow.step1_get_authorize_url()

    def _getUserId(self, svcRec):
        client = self.GetClient(svcRec)
        try:
            about = client.about().get().execute()
            # TODO: Is this a good user ID to use?  Could also use email..
            return about['rootFolderId']
        except errors.HttpError as error:
            raise APIException("Google drive error fetching user ID - %s" % error)

    def RetrieveAuthorizationToken(self, req, level):
        from tapiriik.services import Service
        flow = self._oauthFlow()
        code = req.GET['code']
        credentials = flow.step2_exchange(code)
        cred_json = credentials.to_json()

        # User ID doesn't come back in the credentials.. id_token is null.
        existingRecord = Service.GetServiceRecordWithAuthDetails(self, {"Credentials": cred_json})
        if existingRecord is None:
            uid = self._getUserId(ServiceRecord({"Authorization": {"Credentials": cred_json}}))
        else:
            uid = existingRecord.ExternalID

        return (uid, {"Credentials": cred_json})

    def RevokeAuthorization(self, serviceRec):
        credentials = OAuth2Credentials.from_json(serviceRec.Authorization["Credentials"])
        # should this just be calling credentials.revoke()?
        resp = requests.post(GOOGLE_REVOKE_URI, data={"token": credentials.access_token})
        if resp.status_code == 400:
            try:
                result = json.loads(resp.text)
                if result.get("error") == 'invalid_token':
                    logging.debug("Google drive said token %s invalid when we tried to revoke it, oh well.." % credentials.access_token)
                    # Token wasn't valid anyway, we're good
                    return
            except ValueError:
                raise APIException("Error revoking Google Drive auth token, status " + str(resp.status_code) + " resp " + resp.text)
        elif resp.status_code != 200:
            raise APIException("Unable to revoke Google Drive auth token, status " + str(resp.status_code) + " resp " + resp.text)
        pass

    def _getFileId(self, svcRec, client, path, cache):
        """ get file id for the given path.  Returns None if the path does not exist. """
        if "FileIDs" not in cache:
            cache["FileIDs"] = []
        idCache = cache["FileIDs"]

        if path == '':
            path = '/'

        assert(path.startswith('/'))
        if path.endswith('/'):
            path = path[:-1]
        currentid = 'root'
        parts = path.split('/')
        offset = 1

        while offset < len(parts):
            existingRecord = [x for x in idCache if (x["Parent"] == currentid and x["Name"] == parts[offset])]
            if len(existingRecord):
                existingRecord = existingRecord[0]
                currentid = existingRecord["ID"]
            else:
                try:
                    params = {'q': "title = '%s'" % parts[offset], 'fields': 'items/id'}
                    children = client.children().list(folderId=currentid, **params).execute()
                except errors.HttpError as error:
                    raise APIException("Error listing Google Drive contents - %s" + str(error))

                if not len(children.get('items', [])):
                    return None
                childid = children['items'][0]['id']
                idCache.append({"ID": childid, "Parent": currentid, "Name": parts[offset]})
                currentid = childid
            offset += 1
        return currentid

    def GetFileContents(self, svcRec, client, path, cache):
        """ Return a tuple of (contents, version_number) for a given path. """
        file_id = self._getFileId(svcRec, client, path, cache)
        file = client.files().get(fileId=file_id).execute()
        download_url = file.get('downloadUrl')
        if download_url:
            resp, content = client._http.request(download_url)
            if resp.status == 200:
                return content, file["version"]
            else:
                raise APIException("Google drive error - status %d" % resp.status)
        else:
            # File has no contents on google drive
            return None, 0

    def PutFileContents(self, svcRec, client, path, contents, cache):
        """ Write the contents to the file and return a version number for the newly written file. """
        fname = path.split('/')[-1]
        parent = path[:-(len(fname)+1)]
        logger.debug("Google Drive putting file contents for %s %s" % (parent, fname))
        parent_id = self._getFileId(svcRec, client, parent, cache)

        if parent_id is None:
            # First Need to make a directory.  Only make one level up.
            dirname = parent.split('/')[-1]
            top_parent = parent[:-(len(dirname)+1)]
            logger.debug("Google Drive creating parent - '%s' '%s'" % (top_parent, dirname))
            top_parent_id = self._getFileId(svcRec, client, top_parent, cache)
            if top_parent_id is None:
                raise APIException('Parent of directory for %s does not exist, giving up' % (path,))

            body = {'title': dirname, 'mimeType': FOLDER_MIMETYPE, 'parents': [{'id': top_parent_id}]}

            try:
                parent_obj = client.files().insert(body=body).execute()
                parent_id = parent_obj['id']
            except errors.HttpError as error:
                raise APIException("Google drive error creating folder - %s" % error)

        extn = fname.split('.')[-1].lower()
        if extn not in MIMETYPES:
            # Shouldn't happen?
            raise APIException('Google drive upload only supports file types %s' % (MIMETYPES.keys(),))

        media_body = MediaInMemoryUpload(contents.decode('UTF-8'), mimetype=MIMETYPES[extn], resumable=True)
        body = {'title': fname, 'description': fname, 'mimeType': MIMETYPES[extn]}
        body['parents'] = [{'id': parent_id}]

        try:
            file = client.files().insert(body=body, media_body=media_body).execute()
            return file['version']
        except errors.HttpError as error:
            raise APIException("Google drive upload error - %s" % error)

    def MoveFile(self, svcRec, client, path, destPath, cache):
        """ Move/rename the file 'path' to 'destPath'. """
        fname1 = path.split('/')[-1]
        fname2 = destPath.split('/')[-1]
        if path[:-len(fname1)] != destPath[:-len(fname2)]:
            # Currently only support renaming files in the same dir, otherwise
            # we have to twiddle parents which is hard..
            raise NotImplementedError()

        file_id = self._getFileId(svcRec, client, path, cache)
        try:
            file = client.files().get(fileId=file_id).execute()
            file['title'] = fname1
            client.files().update(fileId=file_id, body=file, newRevision=False).execute()
        except errors.HttpError as error:
            raise APIException("Error renaming file: %s" % error)

    def ServiceCacheDB(self):
        """ Get the cache DB object for this service, eg, self.ServiceCacheDB() """
        return cachedb.googledrive_cache

    def SyncRoot(self, svcRec):
        """ Get the root directory on the service that we will be syncing to, eg, '/tapiriik/' """
        return "/tapiriik"

    def EnumerateFiles(self, svcRec, client, root, cache):
        """ List the files available on the remote (applying some filtering,
        and using ServiceCacheDB as appropriate.  Should yield tuples of:
          (fullPath, relPath, fileid)
        where fileid is some unique id that can be passed back to the functions above.
        """
        root_id = self._getFileId(svcRec, client, root, cache)

        if root_id is None:
            # Root does not exist.. that's ok, just no files to list.
            return

        outlist = []
        self._folderRecurse(svcRec, client, root_id, root, cache, outlist)

        for (path, fileid, rev) in outlist:
            yield (path, path.replace(root, "", 1), fileid, rev)

    def _folderRecurse(self, svcRec, client, parent_id, parent_path, cache, outlist):
        assert(not parent_path.endswith('/'))
        # TODO: Use the cache..
        page_token = None
        while True:
            try:
                param = {'maxResults': 1000, 'q': "trashed = false and '%s' in parents" % parent_id, 'fields': 'items(id,version,parents(id,isRoot,kind),title,md5Checksum,mimeType),kind,nextLink,nextPageToken'}
                if page_token:
                    param['pageToken'] = page_token
                children = client.files().list(**param).execute()

                for child in children.get('items', []):
                    ctitle = child['title']
                    cid = child['id']
                    cpath = parent_path + '/' + ctitle
                    if child.get('mimeType') == FOLDER_MIMETYPE:
                        self._folderRecurse(svcRec, client, cid, cpath, cache, outlist)
                    elif ctitle.lower().endswith(".gpx") or ctitle.lower().endswith(".tcx"):
                        outlist.append((cpath, cid, child["version"]))
                page_token = children.get('nextPageToken')
                if not page_token:
                    break
            except errors.HttpError as error:
                raise APIException("Error listing files in Google Drive - %s" % error)
