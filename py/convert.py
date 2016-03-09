import webapp2
from dronin.logfs import LogFSImport

defs = {
        'Release-20160120.3' : file('static/Release-20160120.3.tgz', 'rb').read()
        }

def GetDefinitions(githash):
    return defs[githash]

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
