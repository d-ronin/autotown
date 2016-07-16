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

        # Super naive hack implementation of d-ronin/dRonin#1019; if a
        # mixer channel is unused, force the PWM range during upgrade to
        # 0/0/0 so that it doesn't affect timer resolution / oneshot
        # update time

        try:
            mixerSettings = imported['UAVO_MixerSettings']

            actuatorSettings = imported['UAVO_ActuatorSettings']
            actuatorSettingsList = list(actuatorSettings)

            mins = list(actuatorSettings.ChannelMin)
            maxes = list(actuatorSettings.ChannelMax)
            newts = list(actuatorSettings.ChannelNeutral)

            for i in range(1,11):
                fieldName = 'Mixer%dType' % (i)

                if mixerSettings[mixerSettings._fields.index(fieldName)] == mixerSettings.ENUM_Mixer1Type['Disabled']:
                    mins[i-1] = 0
                    maxes[i-1] = 0
                    newts[i-1] = 0

            # Fixup to replace fields
            actuatorSettingsList[actuatorSettings._fields.index('ChannelMin')] = mins
            actuatorSettingsList[actuatorSettings._fields.index('ChannelMax')] = maxes
            actuatorSettingsList[actuatorSettings._fields.index('ChannelNeutral')] = newts

            actuatorSettingsNew = actuatorSettings.__class__._make(actuatorSettingsList)

            # if we got here successfully... replace actuatorSettings with
            # our updated one...
            imported['UAVO_ActuatorSettings'] = actuatorSettingsNew
        except:
            logging.exception('Error on attempting to adapt ActuatorSettings')

        self.response.write(imported.ExportXML())

app = webapp2.WSGIApplication([
    ('/convert', UpgraderApp),
], debug=True)

#def main():
#    from paste import httpserver
#    httpserver.serve(app, host='127.0.0.1', port=8080)
#
#if __name__ == '__main__':
#    main()
