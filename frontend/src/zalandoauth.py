#!/usr/bin/env python

import cherrypy
import requests
import os
import json
from urllib import urlencode


FLAG_access_token = 'access_token'

cherrypy.config.update({
    'tools.zalandoauthtool.on': True,
    'tools.sessions.on': True,
    'tools.sessions.timeout': 30,
})

class ZalandOauth(cherrypy.Tool):

    def __init__(self, settings):
        # cherrypy.session is not available before this _point
        self._point = 'before_handler'
        self._name = None
        self._priority = 0
        oauth2 = settings['oauth2']
        self.client_id = oauth2['client_id']
        self.client_secret = oauth2['client_secret']
        self.access_token_url = oauth2['access_token_url']
        self.authorize_url = oauth2['authorize_url']
        self.redirect_url = oauth2['redirect_url']

    def logout(self):
        cherrypy.sessions.expire()
        raise cherrypy.HTTPRedirect('/')
    logout.expose = True

    def zalandoauthtool(self):
        auth_code = cherrypy.request.params.get('code')
        target_url = cherrypy.request.params.get('state')
        if auth_code:
            # get access token
            data = {'grant_type': 'authorization_code', 'code': auth_code
                    , 'redirect_uri': self.redirect_url}
            response = requests.post(self.access_token_url
                                , data=data
                                , auth=(self.client_id, self.client_secret))
            if response.json().get('access_token'):
                access_token = response.json()['access_token']
                cherrypy.session[FLAG_access_token] = access_token
                response.close()
                # redirect to endpoint where user attempted to access
                if target_url:
                    raise cherrypy.HTTPRedirect(target_url)
                else:
                    raise cherrypy.HTTPRedirect('/')
            else:
                response.close()
                raise Exception(response.json())
        if not cherrypy.session.get(FLAG_access_token):
            target_url = cherrypy.url(qs=cherrypy.request.query_string)
            params = {'response_type':'code', 'redirect_uri': self.redirect_url
                        , 'client_id': self.client_id, 'state': target_url}
            raise cherrypy.HTTPRedirect("%s&%s" % (self.authorize_url, urlencode(params)))
        # prevent session regeneration
        # http://docs.cherrypy.org/en/latest/pkg/cherrypy.lib.html?#session-fixation-protection
        cherrypy.session['flag'] = os.urandom(24)

    def callable(self):
        self.zalandoauthtool()

