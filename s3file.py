from urllib.parse import urlparse
import io, mimetypes, os, datetime, boto3, botocore
from botocore.client import ClientError

__version__ = '1.3'


def s3open(*args, **kwargs):
    """ Convenience method for creating S3File object.
    """
    return S3File(*args, **kwargs)


class S3File(object):
    def __init__(self, url, key=None, secret=None, expiration_days=0, private=False, content_type=None, create=True):

        self.url = urlparse(url)
        self.expiration_days = expiration_days
        self.buffer = io.BytesIO()

        self.private = private
        self.closed = False
        self._readreq = True
        self._writereq = False
        self.content_type = content_type or mimetypes.guess_type(self.url.path)[0]

        # bucket = self.url.netloc
        self.bucket_name = self.url.path.split("/")[1]
        print(self.bucket_name)
        # if bucket.endswith('.s3.amazonaws.com'):
        #     bucket = bucket[:-17]

        self.client = boto3.resource('s3')

        self.name = "s3://" + self.bucket_name + '/' + self.url.path.split("/")[-1]
        print(self.name)

        # The boto-specific methods.
        def bucket_exists(bucket_name):
            """
            Returns ``True`` if a bucket exists and you have access to
            call ``HeadBucket`` on it, otherwise ``False``.
            """
            try:
                # http://boto3.readthedocs.org/en/latest/guide/migrations3.html#accessing-a-bucket
                self.client.meta.client.head_bucket(Bucket=bucket_name)
                return True
            except ClientError:
                return False

        self.name = self.url.path.lstrip("/")

        if create and not bucket_exists(self.bucket_name):
            print("creating bucket", self.bucket_name)
            # http://boto3.readthedocs.org/en/latest/guide/migrations3.html#creating-a-bucket
            self.bucket = self.client.Bucket(self.bucket_name)
            self.bucket.create()
        else:
            self.bucket = self.client.Bucket(self.bucket_name)

        #    self.name="serkantest.txt"
        self.key = self.client.Object(self.bucket_name, self.name)

        self.buffer.truncate(0)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def _remote_read(self):
        """ Read S3 contents into internal file buffer.
                        Once only
        """
        if self._readreq:
            print("this is the kye",self.key)
            self.buffer.truncate(0)
            # TODO - BOTO3 METHOD TO CHECK IF FILE EXISTS
            # if self.key.exists():
            #     self.key.get_contents_to_file(self.buffer)
            #x = self.key.get()["Body"].read()
            self.buffer.write(self.key.get()["Body"].read())
            self.buffer.seek(0)
            self._readreq = False

    def _remote_write(self):
        """ Write file contents to S3 from internal buffer.
        """
        if self._writereq:
            self.truncate(self.tell())

            headers = {
                "x-amz-acl": "private" if self.private else "public-read"
            }

            if self.content_type:
                headers["Content-Type"] = self.content_type

            if self.expiration_days:
                now = datetime.datetime.utcnow()
                then = now + datetime.timedelta(self.expiration_days)
                headers["Expires"] = then.strftime("%a, %d %b %Y %H:%M:%S GMT")
                headers["Cache-Control"] = 'max-age=%d' % (self.expiration_days * 24 * 3600,)

            # self.key.set_contents_from_file(self.buffer, headers=headers, rewind=True)
            # http://boto3.readthedocs.org/en/latest/guide/migrations3.html#storing-data

            self.key.put(Body=self.buffer.getvalue())

    def close(self):
        """ Close the file and write contents to S3.
        """
        self._remote_write()
        self.buffer.close()
        self.closed = True

    # pass-through methods

    def flush(self):
        self._remote_write()

    def __next__(self):
        self._remote_read()
        return next(self.buffer)

    def read(self, size=-1):
        self._remote_read()
        return self.buffer.read(size)

    def readline(self, size=-1):
        self._remote_read()
        return self.buffer.readline(size)

    def readlines(self, sizehint=-1):
        self._remote_read()
        return self.buffer.readlines(sizehint)

    def xreadlines(self):
        self._remote_read()
        return self.buffer

    def seek(self, offset, whence=os.SEEK_SET):
        self.buffer.seek(offset, whence)
        # if it looks like we are moving in the file and we have not written
        # anything then we probably should read the contents
        if self.tell() != 0 and self._readreq and not self._writereq:
            self._remote_read()
            self.buffer.seek(offset, whence)

    def tell(self):
        return self.buffer.tell()

    def truncate(self, size=None):
        self._writereq = True
        self.buffer.truncate(size or self.tell())

    def write(self, s):
        self._writereq = True
        self.buffer.write(str.encode(s))

    def writelines(self, sequence):
        self._writereq = True
        self.buffer.writelines(sequence)
