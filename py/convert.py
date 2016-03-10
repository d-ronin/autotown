import zlib
import webapp2
import urllib2
import logging

from dronin.logfs import LogFSImport

class UpgraderApp(webapp2.RequestHandler):
    def get(self):
        self.response.write("""
<form enctype="multipart/form-data" method="post">
<p>
Source githash:<br>
<input type="text" name="githash" size="30">
</p>
<p>
Please specify a file, or a set of files:<br>
<input type="file" name="datafile" size="40">
</p>
<div>
<input type="submit" value="Send">
</div>
</form>
""")

    def post(self):
        self.response.headers['Content-Type'] = 'text/xml'

        try:
            datafile = zlib.decompress(self.request.get('datafile'))
        except:
            logging.exception('Decompression error on user input')
            datafile = self.request.get('datafile')

        imported = LogFSImport(self.request.get('githash'), datafile)

        self.response.write(imported.ExportXML())

app = webapp2.WSGIApplication([
    ('/convert', UpgraderApp),
], debug=True)
