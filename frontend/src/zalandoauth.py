#!/usr/bin/env python

import cherrypy
import requests
import os
from urllib import urlencode


FLAG_access_token = 'access_token'

cherrypy.config.update({
    'tools.zalandoauthtool.on': True,
    'tools.sessions.on': True,
    'tools.sessions.timeout': 30,
})

class ZalandOauth(cherrypy.Tool):

    def __init__(self, oauth2_settings):
        # cherrypy.session is not available before this _point
        self._point = 'before_handler'
        self._name = None
        self._priority = 0
        self.client_id = oauth2_settings['client_id']
        self.client_secret = oauth2_settings['client_secret']
        self.access_token_url = oauth2_settings['access_token_url']
        self.authorize_url = oauth2_settings['authorize_url']
        self.redirect_url = oauth2_settings['redirect_url']

    @cherrypy.expose
    def logout(self):
        cherrypy.lib.sessions.expire()
        raise cherrypy.HTTPRedirect('/')

    def zalandoauthtool(self):
        # the following are used to identify currently flow
        auth_code = cherrypy.request.params.get('code')
        target_url = cherrypy.request.params.get('state')
        scope = cherrypy.request.params.get('scope')
        error = cherrypy.request.params.get('error')
        error_description = cherrypy.request.params.get('error_description')
        if auth_code and scope and target_url:
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
                print response.json()
                response.close()
                raise Exception('Failed to retrieved access-token from server!')
        elif error and error_description:
            print cherrypy.url(qs=cherrypy.request.query_string)
            # in case of error e.g. access-denied we keep the target_url intact
        else:
            # initial case: remember endpoint where user attempted to access
            target_url = cherrypy.url(qs=cherrypy.request.query_string)

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

