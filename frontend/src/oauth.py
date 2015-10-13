#!/usr/bin/env python
# -*- coding: utf-8 -*-

import cherrypy
import requests
import os
from urllib import urlencode

FLAG_access_token = 'access_token'


class Oauth(cherrypy.Tool):

    def __init__(self, oauth_settings):
        # cherrypy.session is not available before this _point
        self._point = 'before_handler'
        self._name = None
        self._priority = 0
        self.client_id = oauth_settings['client_id']
        self.client_secret = oauth_settings['client_secret']
        self.access_token_url = oauth_settings['access_token_url']
        self.authorize_url = oauth_settings['authorize_url']
        self.redirect_url = oauth_settings['redirect_url']
        self.rewrite_http_to_https = oauth_settings['rewrite_http_to_https']
        cherrypy.tools.oauthtool = self

    @cherrypy.expose
    def logout(self):
        cherrypy.lib.sessions.expire()
        raise cherrypy.HTTPRedirect('/')

    def oauthtool(self):
        if not cherrypy.__dict__.get('config', {}).get('tools.sessions.on'):    # to enable skipping /static etc
            return

        # the following are used to identify current state
        auth_code = cherrypy.request.params.get('code')
        target_url = cherrypy.request.params.get('state')
        scope = cherrypy.request.params.get('scope')
        error = cherrypy.request.params.get('error')
        error_description = cherrypy.request.params.get('error_description')
        # user has been redirected back by self.authorize_url
        if auth_code and scope and target_url:
            # get access token
            data = {'grant_type': 'authorization_code', 'code': auth_code, 'redirect_uri': self.redirect_url}
            response = requests.post(self.access_token_url, data=data, auth=(self.client_id, self.client_secret))
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
            # this can occur when, for example, user denies access at self.authorize_url
            # in case of error e.g. access-denied we keep the target_url state intact
            print cherrypy.url(qs=cherrypy.request.query_string)
        else:
            # clean url; no special oauth parameters
            # remember endpoint where user attempts to access; may be passed to self.authorize_url
            if self.rewrite_http_to_https:
                target_url = cherrypy.url(qs=cherrypy.request.query_string)
                target_url = target_url.replace('http://', 'https://')
            else:
                target_url = cherrypy.url(qs=cherrypy.request.query_string)

        # main gate: user must have an access_token to proceed to application
        if not cherrypy.session.get(FLAG_access_token):
            params = {
                'response_type': 'code',
                'redirect_uri': self.redirect_url,
                'client_id': self.client_id,
                'state': target_url,
            }
            raise cherrypy.HTTPRedirect('%s&%s' % (self.authorize_url, urlencode(params)))
        # prevent session regeneration
        # http://docs.cherrypy.org/en/latest/pkg/cherrypy.lib.html?#session-fixation-protection
        cherrypy.session['flag'] = os.urandom(24)

    def callable(self):
        self.oauthtool()


