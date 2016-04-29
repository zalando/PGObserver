import logging
import connexion
import flask
import handlers


def get_hosts(host_id=None):
    host_id = flask.request.args.get('host_id')
    if host_id:
        return [host_id]
    return [1, 2]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # the following line is only needed for OAuth support
    api_args = {}
    app = connexion.App(__name__, port=8080, debug=True, specification_dir='swagger/', swagger_ui=False)
    app.add_api('my_api.yaml', arguments=api_args)
    app.run()
