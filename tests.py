import logging
from optparse import OptionParser

import sys
from botocore.exceptions import ClientError

from s3file import s3open
import random, unittest, boto3

LOREM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit."


class TestS3File(unittest.TestCase):
	def __init__(self, testname, key=None, secret=None):
		super(TestS3File, self).__init__(testname)
		self.key = key
		self.secret = secret
		from datetime import datetime
		self.session_id = "{0}_{1:06d}".format(datetime.now().strftime("%Y%m%dt%H%M%S"),random.randint(0, 999999))
		self.resource = boto3.resource("s3")

	def setUp(self):
		bucket_name = "s3file_{0}".format(self.session_id)
		self.lorem = "{0}/{1}".format(self.session_id, LOREM)
		self.bucket = self.resource.Bucket(bucket_name)
		exists = True
		try:
			# http://boto3.readthedocs.org/en/latest/guide/migrations3.html#accessing-a-bucket
			self.resource.meta.client.head_bucket(Bucket=bucket_name)
			self.bucket.create()
		except ClientError as e:
			error_code = int(e.response["Error"]["Code"])
			if error_code == 404:
				exists = False

		if not exists:
			self.bucket.create()

	def get_url(self, path):
		return "http://{0}.s3.amazonaws.com/{1}".format(self.bucket.name, path.lstrip("/"))

	def test_context_manager(self):
		path = "test_write_cm.txt"

		with s3open(self.get_url(path)) as f:
			f.write(self.lorem)

		k = self.resource.Object(self.bucket.name, path)
		read = str(k.get()["Body"].read(), 'utf-8')
		self.assertEqual(read, self.lorem)

	def test_write(self):
		path = "test_write.txt"

		# write using s3file
		f = s3open(self.get_url(path))
		f.write(self.lorem)
		f.close()

		# check contents using boto
		k = self.resource.Object(self.bucket.name, path)
		self.assertEqual(str(k.get()["Body"].read(), 'utf-8'), self.lorem)

	def test_read(self):
		path = "test_read.txt"

		# write using boto
		k = self.resource.Object(self.bucket.name, path)
		k.put(Body=self.lorem)

		# check contents using s3file
		f = s3open(self.get_url(path))
		self.assertEqual(str(f.read(), 'utf-8'), self.lorem)
		f.close()

	def test_tell(self):
		url = self.get_url("test_tell.txt")
		f = s3open(url)
		f.write(self.lorem)
		f.close()

		f = s3open(url)
		self.assertEqual(str(f.read(8), 'utf-8'), self.lorem[:8])
		self.assertEqual(f.tell(), 8)

	def lorem_est(self):
		lor = self.lorem + "\n"
		lines = [lor, lor[1:] + lor[:1], lor[2:] + lor[:2], lor[3:] + lor[:3],
						 lor[4:] + lor[:4], lor[5:] + lor[:5], lor[6:] + lor[:6]]
		return lines

	def test_readlines(self):
		path = "test_readlines.txt"
		url = self.get_url(path)
		lines = self.lorem_est()
		res = "".join(lines)
		k = self.resource.Object(self.bucket.name, path)
		k.put(Body=res)
		f = s3open(url)
		rlines = f.readlines()
		rres = "".join([str(s, 'utf-8') for s in rlines])
		f.close()
		self.assertEqual(res, rres)

	def test_writelines(self):
		path = "test_writelines.txt"
		url = self.get_url(path)
		f = s3open(url)
		lines = self.lorem_est()
		res = "".join(lines)
		f.writelines(lines)
		f.close()
		k = self.resource.Object(self.bucket.name, path)
		self.assertEqual(str(k.get()["Body"].read(), 'utf-8'), res)

	def test_readline(self):
		path = "test_readline.txt"
		url = self.get_url(path)
		lines = self.lorem_est()
		res = "".join(lines)
		k = self.resource.Object(self.bucket.name, path)
		k.put(Body=res)
		f = s3open(url)
		rline = str(f.readline(), 'utf-8')
		f.close()
		self.assertEqual(rline, self.lorem + "\n")

	def test_closed(self):
		path = "test_closed.txt"
		url = self.get_url(path)
		f = s3open(url)
		self.assertEqual(False, f.closed)
		f.close()
		self.assertEqual(True, f.closed)

	def test_name(self):
		path = "test_name.txt"
		url = self.get_url(path)
		f = s3open(url)
		self.assertEqual("s3://" + self.bucket.name + "/" + path, f.path)
		f.close()

	def test_flush(self):
		path = "test_flush.txt"
		url = self.get_url(path)
		fl = self.lorem + "\n" + self.lorem + "\n"
		fl2 = fl + fl
		f = s3open(url)
		f.write(fl)
		f.flush()
		k = self.resource.Object(self.bucket.name, path)
		self.assertEqual(str(k.get()["Body"].read(), 'utf-8'), fl)
		f.write(fl)
		f.close()
		self.assertEqual(str(k.get()["Body"].read(), 'utf-8'), fl2)

	def test_xreadlines(self):
		path = "test_xreadlines.txt"
		url = self.get_url(path)
		lines = self.lorem_est()
		res = "".join(lines)
		k = self.resource.Object(self.bucket.name, path)
		k.put(Body=res)
		f = s3open(url)
		rres = ""
		for lin in f.xreadlines():
			rres += str(lin, 'utf-8')
		f.close()
		self.assertEqual(res, rres)

	def test_seek(self):
		# needs start, relative, end
		path = "test_seek.txt"
		url = self.get_url(path)
		lines = self.lorem_est()
		res = "".join(lines)
		k = self.resource.Object(self.bucket.name, path)
		k.put(Body=res)
		f = s3open(url)
		f.seek(2, 0)
		self.assertEqual(str(f.read(8), 'utf-8'), res[2:10])
		f.seek(1)
		self.assertEqual(str(f.read(8), 'utf-8'), res[1:9])
		f.seek(-1, 1)
		self.assertEqual(str(f.read(9), 'utf-8'), res[8:17])
		f.seek(-10, 2)
		self.assertEqual(str(f.read(10), 'utf-8'), res[-10:])
		f.close()

	def test_truncate(self):
		path = "test_truncate.txt"
		url = self.get_url(path)
		lines = self.lorem_est()
		res = "".join(lines)
		k = self.resource.Object(self.bucket.name, path)
		k.put(Body=res)
		f = s3open(url)
		dummy = f.read()  # not convinced we should do this but down to the trucate in the write
		f.truncate(3)
		f.close()

		t = s3open(url)
		self.assertEqual(str(t.read(), 'utf-8'), res[:3])
		t.seek(1, 0)
		t.truncate()
		t.close()

		f = s3open(url)
		self.assertEqual(str(f.read(), 'utf-8'), res[:1])
		f.close()

	def _bin_str(self):
		bs = ""
		for i in range(0, 256):
			bs += chr(i)
		return bs

	def test_binary_write(self):
		path = "test_binary_write.txt"
		bs = self._bin_str()
		f = s3open(self.get_url(path))
		f.write(bs)
		f.close()
		k = self.resource.Object(self.bucket.name, path)
		read = str(k.get()["Body"].read(), 'utf-8')
		self.assertEqual(read, bs)

	def test_large_binary_write(self):
		path = "test_large_binary_write.txt"
		bs = self._bin_str()
		for i in range(0, 10):
			bs += bs
		f = s3open(self.get_url(path))
		f.write(bs)
		f.close()
		k = self.resource.Object(self.bucket.name, path)
		self.assertEqual(str(k.get()["Body"].read(), 'utf-8'), bs)

	def test_binary_read(self):
		path = "test_binary_read.txt"
		bs = self._bin_str()
		k = self.resource.Object(self.bucket.name, path)
		k.put(Body=bs)
		url = self.get_url(path)
		f = s3open(url)
		read = str(f.read(), 'utf-8')
		self.assertEqual(read, bs)
		f.close()

	def test_large_binary_read(self):
		path = "test_large_binary_read.txt"
		bs = self._bin_str()
		for i in range(0, 10):
			bs += bs
		k = self.resource.Object(self.bucket.name, path)
		k.put(Body=bs)
		f = s3open(self.get_url(path))
		read = str(f.read(), 'utf-8')
		self.assertEqual(read, bs)
		f.close()

	def tearDown(self):
		for key in self.bucket.objects.all():
			key.delete()
		self.bucket.delete()


if __name__ == "__main__":
	op = OptionParser()
	op.add_option("-k", "--access", dest="access", help="AWS access key (optional if boto config exists)",
								metavar="ACCESS")
	op.add_option("-s", "--secret", dest="secret", help="AWS secret key (optional if boto config exists)",
								metavar="SECRET")

	(options, args) = op.parse_args()

	suite = unittest.TestSuite()
	suite.addTest(TestS3File("test_specific", options.key, options.secret))
	suite.addTest(TestS3File("test_write", options.key, options.secret))
	suite.addTest(TestS3File("test_read", options.key, options.secret))
	suite.addTest(TestS3File("test_tell", options.key, options.secret))
	suite.addTest(TestS3File("test_context_manager", options.key, options.secret))
	suite.addTest(TestS3File("test_readlines", options.key, options.secret))
	suite.addTest(TestS3File("test_writelines", options.key, options.secret))
	suite.addTest(TestS3File("test_readline", options.key, options.secret))
	suite.addTest(TestS3File("test_closed", options.key, options.secret))
	suite.addTest(TestS3File("test_name", options.key, options.secret))
	suite.addTest(TestS3File("test_flush", options.key, options.secret))
	suite.addTest(TestS3File("test_xreadlines", options.key, options.secret))
	suite.addTest(TestS3File("test_seek", options.key, options.secret))
	suite.addTest(TestS3File("test_truncate", options.key, options.secret))
	suite.addTest(TestS3File("test_binary_write", options.key, options.secret))
	suite.addTest(TestS3File("test_large_binary_write", options.key, options.secret))
	suite.addTest(TestS3File("test_binary_read", options.key, options.secret))
	suite.addTest(TestS3File("test_large_binary_read", options.key, options.secret))

	unittest.TextTestRunner().run(suite)
