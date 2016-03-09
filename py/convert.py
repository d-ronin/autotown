import webapp2
import urllib2
from dronin.logfs import LogFSImport

def GetDefinitions(githash):
    url = 'http://dronin-autotown.appspot.com/uavos/' + githash
    result = urllib2.urlopen(url)
    return result.read()

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

        githash = self.request.get('githash')
        datafile = self.request.get('datafile')
        defs = GetDefinitions(githash)

        imported = LogFSImport(self.request.get('githash'), self.request.get('datafile'), deftar=defs)

        self.response.write(imported.ExportXML())

app = webapp2.WSGIApplication([
    ('/convert', UpgraderApp),
], debug=True)
